job_file = "jobs.txt"
server_file = "servers.txt"
depedency_file = "dependencies.txt"
power_cap = 600 #WATTS
energy_cap = 10000 #JOULES
repeat = 2 #PERIODIC TASKS



def read_jobs(file):
    f = open(job_file, "r")
    lines = f.readlines()[1:]
    f.close()
    jobs = dict()

    for line in lines:
        id = int(line[0])
        jobs[id] = list(map(int, line.strip().split(" ")))[1:]

    return jobs


def read_servers(file):
    f = open(server_file, "r")
    lines = f.readlines()[1:]
    f.close()
    servers = dict()

    for line in lines:
        id = int(line[0])
        new_line = line.strip().replace("(", "").replace(")", "").split(" ")
        servers[id] = list(map(int, new_line))[1:]
    
    return servers


def read_dependencies(file):
    f = open(depedency_file, "r")
    lines = f.readlines()[1:]
    f.close()
    dependencies = dict()

    for line in lines:
        task0 = int(line[0])
        task1 = int(list(line.strip()).pop())

        if task0 not in dependencies:
            dependencies[task0] = [task1]
        
        else:
            dependencies[task0].append(task1)
    
    return dependencies


def write_results(schedule, file):
    f = open(file, "w")
    f.write("#jobid serverid start end frequency\n")

    for k, v in schedule.items():
        line = f"{k} {v[0]} {v[1]} {v[2]} {v[3]}\n"
        f.write(line)
    
    f.close()


def FIFO(jobs, servers, dependencies):
    # Sorts jobs by arrival date
    sorted_jobs = {k: v for k, v in sorted(jobs.items(), key=lambda item: item[1][0])}
    schedule = dict()
    server_id = 0
    frequency = 1
    current_date = 0

    for k, v in sorted_jobs.items():
        arrival = v[0]
        w = v[1]
        start = current_date
        end = current_date + w
        current_date = end
        schedule[k] = [server_id, start, end, frequency]
    
    return schedule



if __name__ == "__main__":
    jobs = read_jobs(job_file)
    servers = read_servers(server_file)
    dependencies = read_dependencies(depedency_file)
    test = FIFO(jobs, servers, dependencies)
    print(test)
    write_results(test, "test.txt")
