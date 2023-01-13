JOB_FILE = "jobs.txt"
SERVER_FILE = "servers.txt"
DEPENDENCY_FILE = "dependencies.txt"
POWER_CAP = 600 #WATTS
ENERGY_CAP = 10000 #JOULES
REPEAT = 2 #PERIODIC TASKS



def read_jobs(file):
    f = open(file, "r")
    lines = f.readlines()[1:]
    f.close()
    jobs = dict()

    for line in lines:
        list_line = list(map(int, line.strip().split(" ")))
        id = (list_line[0], 0)
        jobs[id] = list_line[1:]

    return jobs


def read_servers(file):
    f = open(file, "r")
    lines = f.readlines()[1:]
    f.close()
    servers = dict()

    for line in lines:
        new_line = line.strip().replace("(", "").replace(")", "").split(" ")
        list_line = list(map(int, new_line))
        id = list_line[0]
        servers[id] = list_line[1:]
    
    return servers


def read_dependencies(file):
    f = open(file, "r")
    lines = f.readlines()[1:]
    f.close()
    dependencies = dict()

    for line in lines:
        new_line = line.strip().replace("-", "").split("  ")
        list_line = list(map(int, new_line))
        task0, task1 = list_line

        if task0 not in dependencies:
            dependencies[task0] = [task1]
        
        else:
            dependencies[task0].append(task1)
    
    return dependencies


def write_results(schedule, file):
    f = open(file, "w")
    f.write("#jobid serverid start end frequency\n")

    for k, v in schedule.items():
        line = f"{k[0]} {v[0]} {v[1]} {v[2]} {v[3]}\n"
        f.write(line)
    
    f.close()


def add_periodic_jobs(jobs, repeat):
    periodic_jobs = dict()

    for k, v in jobs.items():
        period = v[3]

        if period != 0:
            for i in range(1, repeat + 1):
                new_id = (k[0], i)
                new_arrival_date = period * i + v[0]
                new_deadline = new_arrival_date + v[2]
                new_value = [new_arrival_date, v[1], new_deadline, 0]
                periodic_jobs[new_id] = new_value
    
    return jobs | periodic_jobs


def FIFO(jobs, servers, dependencies):
    sorted_jobs = {k: v for k, v in sorted(jobs.items(), key=lambda item: item[1][0])}  # Sort jobs by arrival date
    schedule = dict()
    server_id = 0
    frequency = 1
    current_date = 0

    for k, v in sorted_jobs.items():
        arrival = v[0]
        w = v[1]
        
        if arrival > current_date:
            start = arrival
        else:
            start = current_date
        
        end = start + w
        current_date = end
        schedule[k] = [server_id, start, end, frequency]
    
    return schedule


def RR(jobs, servers, dependencies, quantum):
    sorted_jobs = {k: v for k, v in sorted(jobs.items(), key=lambda item: item[1][0])}  # Sort jobs by arrival date
    schedule = dict()
    job_ids = [k for k in jobs.keys()]
    server_id = 0
    frequency = 1
    queue = []
    next_arrival = 0
    current_date = 0
    cnt = 0

    while job_ids:
        new_job = job_ids.pop(0)
        queue.append(new_job)

        if job_ids:
            next_job = job_ids[0]
            next_arrival = sorted_jobs[next_job][0]
        else:
            next_arrival = -1

        while next_arrival > current_date and queue:
            current_job = queue.pop(0)
            w = sorted_jobs[current_job][1]
            start = current_date

            if w > quantum:
                sorted_jobs[current_job][1] -= quantum
                current_date += quantum
                queue.append(current_job)

            else:
                current_date += w
            
            schedule[(current_job[0], cnt)] = [server_id, start, current_date, frequency]
            cnt += 1
    
    return schedule



if __name__ == "__main__":
    jobs = read_jobs(JOB_FILE)
    jobs = add_periodic_jobs(jobs, REPEAT)
    servers = read_servers(SERVER_FILE)
    dependencies = read_dependencies(DEPENDENCY_FILE)
    FIFO_schedule = FIFO(jobs, servers, dependencies)
    write_results(FIFO_schedule, "FIFOschedule.txt")
    RR_schedule = RR(jobs, servers, dependencies, 5)
    write_results(RR_schedule, "RRschedule.txt")
