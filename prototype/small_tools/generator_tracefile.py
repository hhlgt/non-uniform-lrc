import random
import math
import sys

def generate_tracefile(testtype, stripe_number):
    line_number = stripe_number
    filename = './../tracefile/' + str(int(testtype)) + '_test' + '.txt'
    if testtype == 0:
        line_number = 1
    with open(filename, 'w') as f:
        for i in range(line_number):
            n = random.randint(2, 3)
            k = []
            for ii in range(n):
                k_i = random.randint(2, 6)
                k.append(k_i)
            object_sizes = []
            for ki in k:
                object_sizes.append(ki)
            w = []
            for ii in range(n):
                w_i = random.randint(1, 50)
                w.append(w_i)
            for ii in range(len(w)):
                para = '(' + str(object_sizes[ii]) + ',' + str(w[ii]) + ')'
                f.write(para)
                if ii != len(w) - 1:
                    f.write(',')
            f.write('\n')
    f.close()

def random_split(k, n, avg):
    result = []
    lower = max(math.floor(3 * avg / 4), 1)
    upper = math.floor(4 * avg / 3)
    last_point = 0
    for i in range(n):
        split_point = int(random.uniform(lower, upper))
        ki = split_point - last_point
        lower = split_point + 1
        upper += avg
        if i == n - 1:
            ki = k - last_point
        result.append(ki)
        last_point = split_point
    return result

def random_intialize_by_split(K, R, g, rate, stripe_number, flag=True):
    l = int((K + g + R - 1) / R)
    x = round(float(K + g + l) / float(K), 3)
    filename = './../tracefile/' + str(x) + '_' + str(K) + '_' + str(R) + '_' + str(g) + '_' + str(rate) + '_' + str(stripe_number) + '.txt'    
    # n_max = math.floor(K / 4)
    # n = random.randint(2, min(n_max, 10))
    n = 5
    fl_avg = K / n
    with open(filename, 'w') as f:
        for i in range(stripe_num):
            k = random_split(K, n, fl_avg)
            random.shuffle(k)
            w = []
            if flag:
                if rate == 0:
                    for i in range(n):  # random file distribution
                        w_i = random.randint(1, 50)
                        w.append(w_i)
                else:
                    for i in range(n):
                        w_i = 0
                        ran_num = random.randint(1, 100)
                        if ran_num < 100 * rate:
                            w_i = random.randint(46, 50)
                        else:
                            w_i = random.randint(1, 5)
                        w.append(w_i)
            else:
                if rate == 0:
                    rate = random.randint(1, 100) / 100
                hot_file_num = math.ceil(n * rate)
                for i in range(n):
                    w_i = 0
                    if i < hot_file_num:
                        w_i = random.randint(46, 50)
                    else:
                        w_i = random.randint(1, 5)
                    w.append(w_i)
            random.shuffle(w)
            for ii in range(len(w)):
                para = '(' + str(k[ii]) + ',' + str(w[ii]) + ')'
                f.write(para)
                if ii != len(w) - 1:
                    f.write(',')
            f.write('\n')
    f.close()

def generate_randomly_in_range(x_low, x_up, rate, stripe_number, flag=True):
    filename = './../tracefile/' + 'wl_' + str(x_low) + '-' + str(x_up) + '_' + str(rate) + '_' + str(stripe_number) + '.txt'
    with open(filename, 'w') as f:
        for i in range(stripe_num):
            factor = random.randint(3, 30)
            K = 4 * factor
            g = random.randint(2, 4)
            x_0 = float((g + K + 2) / K)
            x_0 = max(x_0, x_low + 0.001)
            x = round(random.uniform(x_0, x_up), 3)
            if x < x_0:
                x = x_0
            x_g = str(x) + ',' + str(g) + ','
            f.write(x_g)
            n = 5
            fl_avg = K / n
            k = random_split(K, n, fl_avg)
            random.shuffle(k)
            w = []
            if flag:
                if rate == 0:
                    for i in range(n):  # random file distribution
                        w_i = random.randint(1, 50)
                        w.append(w_i)
                else:
                    for i in range(n):
                        w_i = 0
                        ran_num = random.randint(1, 100)
                        if ran_num < 100 * rate:
                            w_i = random.randint(46, 50)
                        else:
                            w_i = random.randint(1, 5)
                        w.append(w_i)
            else:
                if rate == 0:
                    rate = random.randint(1, 100) / 100
                hot_file_num = math.ceil(n * rate)
                for i in range(n):
                    w_i = 0
                    if i < hot_file_num:
                        w_i = random.randint(46, 50)
                    else:
                        w_i = random.randint(1, 5)
                    w.append(w_i)
            random.shuffle(w)
            for ii in range(len(w)):
                para = '(' + str(k[ii]) + ',' + str(w[ii]) + ')'
                f.write(para)
                if ii != len(w) - 1:
                    f.write(',')
            f.write('\n')
    f.close()


if len(sys.argv) == 4 or len(sys.argv) == 7 or len(sys.argv) == 8:
    func = int(sys.argv[1])
    stripe_num = int(sys.argv[2])
    flag = (sys.argv[3] == "True")
    if func == 1 and len(sys.argv) == 4:
        generate_tracefile(1, stripe_num)
    elif func == 2 and len(sys.argv) == 8:
        rate = float(sys.argv[4])
        K = int(sys.argv[5])
        R = int(sys.argv[6])
        g = int(sys.argv[7])
        random_intialize_by_split(K, R, g, rate, stripe_num, flag)
    elif func == 3 and len(sys.argv) == 7:
        rate = float(sys.argv[4])
        x_low = float(sys.argv[5])
        x_up = float(sys.argv[6])
        generate_randomly_in_range(x_low, x_up, rate, stripe_num, flag)
else:
    print('Invalid arguments! Usage. ')
    print('->$ python generate_tracefile.py 1 stripe_num flag')
    print('->$ python generate_tracefile.py 2 stripe_num flag rate K R g')
    print('->$ python generate_tracefile.py 3 stripe_num flag rate x_low x_up')
