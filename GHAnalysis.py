import json
import os
import argparse
import time
import mmap
from multiprocessing import Pool, cpu_count
import shutil


class Data:
    # 初始化统计好的json数据
    def __init__(self, dict_address: str = None, reload: int = 0):
        if reload == 1:  # 是否重新初始化
            try:
                os.makedirs('temp')
            except:
                shutil.rmtree('temp')
                os.makedirs('temp')
            self.init(dict_address)
        # 判断是否存在完整的统计数据 或者 路径不存在
        if dict_address is None and \
                not os.path.exists('U_E.json') and not os.path.exists('R_E.json') and not os.path.exists('U_R_E.json'):
            raise RuntimeError("Error: init failed")

        # 数据检测完毕，读入数据
        x = open('U_E.json', 'r', encoding='utf-8').read()
        self.__U_E = json.loads(x)
        x = open('R_E.json', 'r', encoding='utf-8').read()
        self.__R_E = json.loads(x)
        x = open('U_R_E.json', 'r', encoding='utf-8').read()
        self.__U_R_E = json.loads(x)

    # 重新初始化
    def init(self, dict_address: str):
        self.__U_E = {}
        self.__R_E = {}
        self.__U_R_E = {}

        # 遍历所有json文件
        for root, dic, files in os.walk(dict_address):
            pool = Pool(processes=max(cpu_count(), 6))  # 调用进程池
            for f in files:
                if f[-5:] == '.json':
                    pool.apply_async(self.one_file_read_in, args=(f, dict_address))
            pool.close()
            pool.join()

        # calculate，统计次数
        for root, dic, files in os.walk("temp"):
            for f in files:
                if f[-5:] == '.json':
                    json_list = json.loads(open("temp" + '\\' + f, 'r', encoding='utf-8').read())
                    for item in json_list:
                        # 类型不符就跳过
                        if not self.__U_E.get(item['actor__login'], 0):
                            self.__U_E.update({item['actor__login']: {}})
                            self.__U_R_E.update({item['actor__login']: {}})
                        self.__U_E[item['actor__login']][item['type']] = \
                            self.__U_E[item['actor__login']].get(item['type'], 0) + 1

                        if not self.__R_E.get(item['repo__name'], 0):
                            self.__R_E.update({item['repo__name']: {}})
                        self.__R_E[item['repo__name']][item['type']] = \
                            self.__R_E[item['repo__name']].get(item['type'], 0) + 1

                        if not self.__U_R_E[item['actor__login']].get(item['repo__name'], 0):
                            self.__U_R_E[item['actor__login']].update({item['repo__name']: {}})
                        self.__U_R_E[item['actor__login']][item['repo__name']][item['type']] = \
                            self.__U_R_E[item['actor__login']][item['repo__name']].get(item['type'], 0) + 1

        # 存储结果，以便查询时调用
        with open('U_E.json', 'w', encoding='utf-8') as f:
            json.dump(self.__U_E, f)
        with open('R_E.json', 'w', encoding='utf-8') as f:
            json.dump(self.__R_E, f)
        with open('U_R_E.json', 'w', encoding='utf-8') as f:
            json.dump(self.__U_R_E, f)

        return "Init Finish"

    # 读取json文件，按行拆分数据
    def one_file_read_in(self, f, dict_address):
        json_list = []
        if f[-5:] == '.json':
            json_path = f
            x = open(dict_address + '\\' + json_path, 'r', encoding='utf-8')
            with mmap.mmap(x.fileno(), 0, access=mmap.ACCESS_READ) as m:
                m.seek(0, 0)
                obj = m.read()
                obj = str(obj, encoding="utf-8")
                str_list = [_x for _x in obj.split('\n') if len(_x) > 0]
                for _str in str_list:
                    try:
                        json_list.append(json.loads(_str))
                    except:
                        pass
            self.one_file_save_in(json_list, f)

    # 存储单个预处理过的json文件，去除除了四个事件之外的事件类型，并重新整理数据
    def one_file_save_in(self, json_list, filename):
        batch_message = []
        for item in json_list:
            if item['type'] not in ["PushEvent", "IssueCommentEvent", "IssuesEvent", "PullRequestEvent"]:
                continue
            batch_message.append({'actor__login': item['actor']['login'],
                                  'type': item['type'],
                                  'repo__name': item['repo']['name']}
                                 )
        with open('temp\\' + filename, 'w', encoding='utf-8') as F:
            json.dump(batch_message, F)

    # 查询用户——事件
    def get_events_users(self, username: str, event: str) -> int:
        if not self.__U_E.get(username, 0):
            return 0
        else:
            return self.__U_E[username].get(event, 0)

    # 查询项目——事件
    def get_events_repos(self, reponame: str, event: str) -> int:
        if not self.__R_E.get(reponame, 0):
            return 0
        else:
            return self.__R_E[reponame].get(event, 0)

    # 查询用户——项目——事件
    def get_events_users_repos(self, username: str, reponame: str, event: str) -> int:
        if not self.__U_E.get(username, 0):
            return 0
        elif not self.__U_R_E[username].get(reponame, 0):
            return 0
        else:
            return self.__U_R_E[username][reponame].get(event, 0)


# 运行类，包含初始化、读取参数、返回参数
class Run:
    #  初始化配置，并统计用时
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.data = None
        self.arg_init()
        t_start = time.time()
        print(self.analyse())
        t_end = time.time()
        print(t_end - t_start)

    #  初始化命令参数
    def arg_init(self):
        self.parser.add_argument('-i', '--init')
        self.parser.add_argument('-u', '--user')
        self.parser.add_argument('-r', '--repo')
        self.parser.add_argument('-e', '--event')

    #  分析参数
    def analyse(self):
        if self.parser.parse_args().init:
            self.data = Data(self.parser.parse_args().init, 1)
            return "Init Finish"
        else:
            if self.data is None:
                self.data = Data()
            if self.parser.parse_args().event:
                if self.parser.parse_args().user:
                    if self.parser.parse_args().repo:
                        res = self.data.get_events_users_repos(
                            self.parser.parse_args().user,
                            self.parser.parse_args().repo,
                            self.parser.parse_args().event
                        )
                    else:
                        res = self.data.get_events_users(
                            self.parser.parse_args().user,
                            self.parser.parse_args().event
                        )
                elif self.parser.parse_args().repo:
                    res = self.data.get_events_repos(
                        self.parser.parse_args().repo,
                        self.parser.parse_args().event
                    )
                else:
                    raise RuntimeError("Error: argument [ -u|--user ] or [ -r|--repo ] cannot be found!")
            else:
                raise RuntimeError("Error: argument [ -e|--event ] cannot be found!")
        return res


if __name__ == "__main__":
    Run()
