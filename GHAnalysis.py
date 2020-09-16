import json
import os
import argparse
import time
import mmap
from multiprocessing import Pool, cpu_count


class Data:
    # 初始化统计好的json数据
    def __init__(self, dict_address: str = None, reload: int = 0):
        if reload == 1:  # 是否重新初始化
            self.__init(dict_address)
        # 判断是否存在完整的统计数据 或者 路径不存在
        if dict_address is None or \
                not os.path.exists('U_E.json') or not os.path.exists('R_E.json') or not os.path.exists('U_R_E.json'):
            raise RuntimeError("Error: init failed, please init again ...")

        # 数据检测完毕，读入数据
        x = open('U_E.json', 'r', encoding='utf-8').read()
        self.__U2E = json.loads(x)
        x = open('R_E.json', 'r', encoding='utf-8').read()
        self.__R2E = json.loads(x)
        x = open('U_R_E.json', 'r', encoding='utf-8').read()
        self.__UR2E = json.loads(x)

    # 读取单个json文件
    def read(self, json_path, dict_address):
        json_list = []
        filename = dict_address + '\\' + json_path
        x = open(filename, 'r', encoding='utf-8')
        with mmap.mmap(x.fileno(), 0, access=mmap.ACCESS_READ) as m:  # mmap快速存储
            m.seek(0, 0)
            obj = m.read()
            obj = str(obj, encoding="utf-8")
            str_list = [_x for _x in obj.split('\n') if len(_x) > 0]
            for _str in str_list:
                json_list.append(json.loads(_str))
        self.store_in_mem(json_list, filename)

    # 存储预处理过的json文件，去除除了四个事件之外的事件类型，并重新整理数据的键值对
    def store_in_mem(self, json_list, filename):
        batch_message = []
        for item in json_list:
            # 类型不符就跳过
            if item['type'] not in ["PushEvent", "IssueCommentEvent", "IssuesEvent", "PullRequestEvent"]:
                continue
            batch_message.append({'actor__login': item['actor']['login'], 'type': item['type'], 'repo__name': item['repo']['name']})
        with open('json_temp\\' + filename, 'w', encoding='utf-8') as F:
            json.dump(batch_message, F)

    # 重新初始化
    def __init(self, dict_address: str):
        self.__U2E = {}
        self.__R2E = {}
        self.__UR2E = {}

        # 遍历所有json文件
        for root, dic, files in os.walk(dict_address):
            # print(cpu_count())
            pool = Pool(processes=max(cpu_count(), 6))  # 调用进程池
            for filename in files:
                if filename[-5:] == '.json':
                    pool.apply_async(self.read, args=(filename, dict_address))  # 分配进程
            pool.close()
            pool.join()

        # calculate，统计次数
        for root, dic, files in os.walk("json_temp"):
            for filename in files:
                if filename[-5:] == '.json':
                    json_list = json.loads(open("json_temp" + '\\' + filename, 'r', encoding='utf-8').read())
                    for item in json_list:
                        # U_E
                        if not self.__U2E.get(item['actor__login'], 0):
                            self.__U2E.update({item['actor__login']: {}})
                            self.__UR2E.update({item['actor__login']: {}})
                        self.__U2E[item['actor__login']][item['type']] = \
                            self.__U2E[item['actor__login']].get(item['type'], 0) + 1

                        # R_E
                        if not self.__R2E.get(item['repo__name'], 0):
                            self.__R2E.update({item['repo__name']: {}})
                        self.__R2E[item['repo__name']][item['type']] = \
                            self.__R2E[item['repo__name']].get(item['type'], 0) + 1

                        # U_R_E
                        if not self.__UR2E[item['actor__login']].get(item['repo__name'], 0):
                            self.__UR2E[item['actor__login']].update({item['repo__name']: {}})
                        self.__UR2E[item['actor__login']][item['repo__name']][item['type']] = \
                            self.__UR2E[item['actor__login']][item['repo__name']].get(item['type'], 0) + 1

        # 存储结果，以便查询时调用
        with open('U_E.json', 'w', encoding='utf-8') as f:
            json.dump(self.__U2E, f)
        with open('R_E.json', 'w', encoding='utf-8') as f:
            json.dump(self.__R2E, f)
        with open('U_R_E.json', 'w', encoding='utf-8') as f:
            json.dump(self.__UR2E, f)
    
    # 无效的嵌套字典重构代码
    # def __parseDict(self, d: dict, prefix: str):
    #     _d = {}
    #     for k in d.keys():
    #         if str(type(d[k]))[-6:-2] == 'dict':
    #             _d.update(self.__parseDict(d[k], k))
    #         else:
    #             _k = f'{prefix}__{k}' if prefix != '' else k
    #             _d[_k] = d[k]
    #     return _d
    #
    # def __listOfNestedDict2ListOfDict(self, a: list):
    #     records = []
    #     for d in a:
    #         _d = self.__parseDict(d, '')
    #         print(_d)
    #         records.append(_d)
    #     return records

    # 查询用户——事件
    def get_events_users(self, username: str, event: str) -> int:
        if not self.__U2E.get(username, 0):
            return 0
        else:
            self.cache()
            return self.__U2E[username].get(event, 0)

    # 查询项目——事件
    def get_events_repos(self, repo_name: str, event: str) -> int:
        if not self.__R2E.get(repo_name, 0):
            return 0
        else:
            self.cache()
            return self.__R2E[repo_name].get(event, 0)

    # 查询用户——项目——事件
    def get_events_users_and_repos(self, username: str, repo_name: str, event: str) -> int:
        if not self.__U2E.get(username, 0) or not self.__UR2E[username].get(repo_name, 0):
            return 0
        else:
            self.cache()
            return self.__UR2E[username][repo_name].get(event, 0)

    # 加速查询（未写完）
    def cache(self):

        return


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
        print("T = %lf" % (t_start - t_end))

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
            return "Initial Finish ... Request Ready ... "
        else:
            if self.data is None:
                self.data = Data()
            if self.parser.parse_args().event:  # -e
                if self.parser.parse_args().user:  # -u
                    if self.parser.parse_args().repo:  # -r
                        res = self.data.get_events_users_and_repos(
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
                    raise RuntimeError("Error: argument [ -u | --user ] or [ -r | --repo ] cannot be found!")
            else:
                raise RuntimeError("Error: argument [ -e | --event ] cannot be found!")
        return res



if __name__ == '__main__':
    Run()
