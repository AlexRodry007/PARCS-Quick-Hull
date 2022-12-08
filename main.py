from Pyro4 import expose
from random import randrange

class Solver:
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers
        print("Inited")

    def solve(self):
        print("Job Started")
        print("Workers %d" % len(self.workers))

        dots = self.read_input()
        step = int(len(dots) / len(self.workers))
        mapped = []

        #print("Dots:")
        #print(dots)

        for i in list(range(0, len(self.workers))):
            print("Current worker: " + str(i))
            if i < len(self.workers) - 1:
                addition = self.workers[i].mymap(dots[i * step:(i + 1) * step])
                #print("Addition:")
                #print(addition)
                mapped.append(addition)
                #mapped += addition
            else:
                addition = self.workers[i].mymap(dots[i * step:])
                #print("Addition:")
                #print(addition)
                mapped.append(addition)
                #mapped += addition

        #print("MAPPED:")
        #print(mapped)

        result = self.myreduce(mapped)
        self.write_output(result)
        print("Job finished")

    @staticmethod
    @expose
    def mymap(list_of_dots):
        #print("Worker mymap list of dots:")
        #print(list_of_dots)
        search_quick_hull = QuickHull()
        answer = search_quick_hull.full_quick_hull(search_quick_hull, list_of_dots)
        #print("Worker mymap answer:")
        #print(answer)
        return answer

    @staticmethod
    @expose
    def myreduce(mapped):
        search_quick_hull = QuickHull()
        output = []
        for x in mapped:
            output += x.value
        answer = search_quick_hull.full_quick_hull(search_quick_hull, output)
        return answer

    def read_input(self):
        f = open(self.input_file_name, 'r')
        list_of_dots = []
        for line in f:
            list_of_dots.append([int(line.split(',')[0]), int(line.split(',')[1].rstrip('\n'))])
        f.close()
        return list_of_dots

    def write_output(self, answer):
        open(self.output_file_name, 'w').close()
        f = open(self.output_file_name, 'a')
        for dot in answer:
            f.write(str(dot[0]) + "," + str(dot[1]) + '\n')
        f.close()


class Line:
    def __init__(self, first_dot, second_dot):
        x1 = float(first_dot[0])
        y1 = float(first_dot[1])
        x2 = float(second_dot[0])
        y2 = float(second_dot[1])
        if(first_dot[0] - second_dot[0]) != 0:
            self.simple = True
            self.a = (y1 - y2) / (x1 - x2)
            self.b = (y2 * x1 - y1 * x2) / (x1 - x2)
        else:
            self.simple = False
            self.left_side = first_dot[1] < second_dot[1]
            self.c = first_dot[0]

    def calculate_relative_height(self, dot):
        if self.simple:
            return round(dot[1] - self.a * dot[0] - self.b, 5)
        else:
            if self.left_side:
                return self.c-dot[0]
            else:
                return dot[0]-self.c


class QuickHull:
    @staticmethod
    @expose
    def search_furthest_dots(dots):
        left = dots[0]
        right = dots[0]
        i = 0
        for dot in dots:
            if dot[0] < left[0]:
                left = dot
            if dot[0] > right[0]:
                right = dot
            i += 1
        return [left, right]

    @staticmethod
    @expose
    def half_quick_hull(qh, first_dot, second_dot, dots, top=True):
        #print(first_dot)
        #print(second_dot)
        if first_dot[0] == second_dot[0]:
            return []
        dots_on_one_side = []
        if top:
            line = Line(first_dot, second_dot)
            modifier = 1
        else:
            modifier = -1
            line = Line(second_dot, first_dot)
        max_height = 0
        max_dot = []
        for dot in dots:
            dot_height = line.calculate_relative_height(dot) * modifier
            if dot_height > 0:
                dots_on_one_side.append(dot)
            if dot_height > max_height:
                max_height = dot_height
                max_dot = dot
        if max_height == 0 or (max_dot[0]==first_dot[0] and max_dot[1]==first_dot[1]) or (max_dot[0]==second_dot[0] and max_dot[1]==second_dot[1]):
            return []
        else:
            #print([max_dot])
            #print(max_height)
            return qh.half_quick_hull(qh, first_dot, max_dot, dots_on_one_side, top) + [max_dot] + qh.half_quick_hull(
                qh, max_dot, second_dot, dots_on_one_side, top)

    @staticmethod
    @expose
    def full_quick_hull(qh, dots):
        furthest_dots = qh.search_furthest_dots(dots)
        left = furthest_dots[0]
        right = furthest_dots[1]
        return [left] + qh.half_quick_hull(qh, left, right, dots, True) + [right] + list(reversed(qh.half_quick_hull(qh,
                                                                                                                     left,
                                                                                                                     right,
                                                                                                                     dots,
                                                                                                                     False)))


def randomise_input(file, amount):
    open(file, 'w').close()
    f = open(file, 'a')
    for i in range(amount):
        f.write(str(randrange(10000000)) + "," + str(randrange(10000000)) + '\n')
    f.close()


if __name__ == '__main__':
    randomise_input("input.txt", 500000)
    """
    f = open("input.txt", 'r')
    list_of_dots = []
    for line in f:
        list_of_dots.append([int(line.split(',')[0]), int(line.split(',')[1].rstrip('\n'))])
    f.close()
    worker_length = 3
    step = int(len(list_of_dots) / worker_length)
    mapped = []
    for i in range(0, worker_length):
        print("Current worker: " + str(i))
        if i < worker_length - 1:
            addition = Solver.mymap(list_of_dots[i * step:(i + 1) * step])
            print("Addition:")
            print(addition)
            mapped += addition
        else:
            addition = Solver.mymap(list_of_dots[i * step:])
            print("Addition:")
            print(addition)
            mapped += addition
    print(mapped)
    answer = Solver.mymap(mapped)
    print(answer)
    """
