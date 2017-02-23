#!/usr/bin/env python3
from os import path
import multiprocessing

PATH = path.dirname(__file__)


def p(name):
    return path.join(PATH, name)


INPUTS = [p('kittens.in'), p('me_at_the_zoo.in'),
          p('trending_today.in'), p('videos_worth_spreading.in')]
OUTPUTS = [p('kittens.out'), p('me_at_the_zoo.out'),
           p('trending_today.out'), p('videos_worth_spreading.out')]

cacheservers = []
endpoints = []
videos = []


class CacheServer():
    def __init__(self, id, capacity):
        self.capacity = capacity
        self.id = id
        self.endpoints = []
        self.videos = []

    def add_endpoint(self, endpoint):
        self.endpoints.append(endpoint)

    def add_video(self, video):
        self.videos.append(video)
        self.capacity -= video.mb


class Endpoint():
    def __init__(self, id, latency_datacenter):
        self.id = id
        self.latency_datacenter = latency_datacenter
        self.cacheservers = {}
        self.requests = []

    def add_cacheserver(self, cacheserver, latency):
        self.cacheservers[cacheserver] = latency
        cacheserver.add_endpoint(self)

    def get_video_quantity(self, video):
        for r in self.requests:
            if r.video == video:
                return r.quantity

    def has_video(self, video):
        for r in self.requests:
            if r.video == video:
                return True
        return False


class Request():
    def __init__(self, video, quantity):
        self.video = video
        self.quantity = quantity

    @property
    def weight(self):
        return self.video.mb * self.quantity


class Video():
    def __init__(self, id, mb):
        self.id = id
        self.mb = mb


def get_cacheserver(cacheservers, id):
    for c in cacheservers:
        if c.id == id:
            return c

    raise Exception("No cacheserver %d" % id)


def get_endpoint(endpoints, id):
    for e in endpoints:
        if e.id == id:
            return e

    raise Exception("No endpoint %d" % id)


def get_video(videos, id):
    for v in videos:
        if v.id == id:
            return v

    raise Exception("No video %d" % id)


def get_line(f):
    return [int(x) for x in f.readline().split(' ')]


def write_line(f, line):
    f.write(str(line) + '\n')


def parse_input(i):
    global cacheservers
    global videos
    global endpoints

    with open(i) as f:
        # parse first line
        V, E, R, C, X = get_line(f)
        print("File {} -> V: {}, E: {}, E: {}, C: {}".format(i, V, E, R, C))

        # init cache servers
        cacheservers = []
        for i in range(C):
            cacheservers.append(CacheServer(i, X))

        # init videos
        videos = []
        for i, v in enumerate(get_line(f)):
            videos.append(Video(i, v))

        # init endpoints
        endpoints = []
        for i in range(E):
            Ld, K = get_line(f)
            endpoint = Endpoint(i, Ld)
            endpoints.append(endpoint)
            for j in range(K):
                c, Lc = get_line(f)
                cacheserver = get_cacheserver(cacheservers, c)
                endpoint.add_cacheserver(cacheserver, Lc)

        # init requests
        for i in range(R):
            Rv, Re, Rn = get_line(f)
            endpoint = get_endpoint(endpoints, Re)
            video = get_video(videos, Rv)
            endpoint.requests.append(Request(video, Rn))


def output(output_file):
    cacheservers_with_videos = []
    for cacheserver in cacheservers:
        if len(cacheserver.videos) > 0:
            cacheservers_with_videos.append(cacheserver)

    with open(output_file, 'w') as f:
        write_line(f, len(cacheservers_with_videos))
        for cache in cacheservers_with_videos:
            cache_id = cache.id
            videos_id = [v.id for v in cache.videos]
            write_line(f, str(cache_id) + ' ' + ' '.join(
                [str(x) for x in videos_id]))


def algo1():
    for endpoint in endpoints:
        for request in endpoint.requests:
            min_latency = 9999999999
            selected_cache = None
            for cache, latency in endpoint.cacheservers.items():
                if request.video in cache.videos:
                    continue

                if cache.capacity >= request.weight:
                    current_latency = request.quantity * latency
                    if current_latency <= min_latency:
                        selected_cache = cache
                        min_latency = current_latency
            if selected_cache:
                selected_cache.add_video(request.video)


def algo2():
    # loop through all videos
    videos = []
    for e in endpoints:
        for r in e.requests:
            videos.append(r.video)
    print("1 - List videos")

    videos_weight = {}
    for video in videos:
        sum_demand = 0
        for e in endpoints:
            for r in e.requests:
                if r.video == video:
                    sum_demand += r.quantity

        videos_weight[sum_demand] = video
    print("2 - Sort videos")

    # loop through all video in reversed order (weight of the video)
    for key in reversed(sorted(videos_weight.keys())):
        video = videos_weight[key]
        for e in endpoints:
            if not e.has_video(video):
                continue

            min_latency = 9999999999
            selected_cache = None
            for cache, latency in e.cacheservers.items():
                if video in cache.videos:
                    continue

                current_latency = latency

                if cache.capacity >= video.mb:
                    if current_latency <= min_latency:
                        selected_cache = cache
                        min_latency = current_latency

            if selected_cache:
                selected_cache.add_video(video)


def exec_algo(input_path, output_path):
    parse_input(input_path)
    algo2()
    output(output_path)
    print("End of the file: %s" % input_path)


def main():
    for i, inp in enumerate(INPUTS):
        p = multiprocessing.Process(target=exec_algo, args=(inp, OUTPUTS[i]))
        p.start()


if __name__ == '__main__':
    main()
