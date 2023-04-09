JOB_FILE = "jobs.txt"
SERVER_FILE = "servers.txt"
DEPENDENCY_FILE = "dependencies.txt"
POWER_CAP = 600 #WATTS
ENERGY_CAP = 10000 #JOULES
REPEAT = 0 #PERIODIC TASKS



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

    for job in schedule:
        line = f"{job[0]} {job[1]} {job[2]} {job[3]} {job[4]}\n"
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
    schedule = []
    server_id = 0
    frequency = 1
    current_date = 0

    for job_id, v in sorted_jobs.items():
        arrival_date, w, _, _ = v
        
        if arrival_date > current_date:
            start = arrival_date
        else:
            start = current_date
        
        end = start + w
        current_date = end
        schedule.append([job_id[0], server_id, start, end, frequency])
    
    return schedule


def RR(jobs, servers, dependencies, quantum):
    jobs_cpy = jobs.copy()
    schedule = []
    queue = [k for k in jobs_cpy.keys()]
    server_id = 0
    frequency = 1
    current_date = 0

    while queue:
        job_id = queue.pop(0)
        arrival_date, w, deadline, period = jobs_cpy[job_id]

        if arrival_date > current_date:
            start = arrival_date

        else:
            start = current_date

        if w <= quantum:
            end = start + w

        else:
            end = start + quantum
            queue.append(job_id)
            jobs_cpy[job_id] = [arrival_date, w - quantum, deadline, period]
        
        schedule.append([job_id[0], server_id, start, end, frequency])
        current_date = end
    
    return schedule


def EDF(jobs, servers, dependencies):
    schedule = []
    current_date = 0
    server_id = 0
    frequency = 1
    unfinished_jobs = jobs.copy()

    def get_earliest_deadline_job(unfinished_jobs):
        earliest_deadline = float('inf')
        earliest_deadline_job = None

        for job_id, job in unfinished_jobs.items():
            deadline = job[2]

            if deadline < earliest_deadline:
                earliest_deadline = deadline
                earliest_deadline_job = job_id
            
        return earliest_deadline_job

    while unfinished_jobs:
        job_id = get_earliest_deadline_job(unfinished_jobs)
        job = unfinished_jobs.pop(job_id)

        arrival_date = job[0]
        w = job[1]

        start = max(current_date, arrival_date)
        end = start + w

        current_date = end

        schedule.append([job_id[0], server_id, start, end, frequency])
    
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
    EDF_schedule = EDF(jobs, servers, dependencies)
    write_results(EDF_schedule, "EDFschedule.txt")
