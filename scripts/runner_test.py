import datetime
import os
import sys
import time
import pathlib
import nbformat
import yaml
import mig_meow as meow

VGRID = 'test_vgrid'
CREATE_TEXT = "create: "

SINGLE_PATTERN_MULTIPLE_FILES = 'single_Pattern_multiple_files'
SINGLE_PATTERN_SINGLE_FILE_PARALLEL = 'single_Pattern_single_file_parallel_jobs'
SINGLE_PATTERN_SINGLE_FILE_SEQUENTIAL = 'single_Pattern_single_file_sequential_jobs'
MULTIPLE_PATTERNS_SINGLE_FILE = 'multiple_Patterns_single_file'
MULTIPLE_PATTERNS_MULTIPLE_FILES = 'multiple_Patterns_multiple_files'


results_dir = f"{os.path.sep}results"

def datetime_to_timestamp(date_time_obj):
    return time.mktime(date_time_obj.timetuple()) + float(date_time_obj.microsecond)/1000000 

def generate(file_count, file_path, file_type='.txt'):
    first_filename = ''
    start = time.time()
    for i in range(int(file_count)):
        filename = file_path + str(i) + file_type
        if not first_filename:
            first_filename = filename
        with open(filename, 'w') as f:
            f.write('0')
    return first_filename, time.time() - start

def cleanup(jobs, file_out, base_time, gen_time, execution=False):
    if not jobs:
        return

    job_timestamps = []
    for job in jobs:
        with open(f"/scripts/.workflow_processing/{job}/job.yml", 'r') as f_in:
            data = yaml.load(f_in, Loader=yaml.Loader)
        create_datetime = data['create']
        create_timestamp = datetime_to_timestamp(create_datetime)
        job_timestamps.append((create_timestamp, create_datetime))

    job_timestamps.sort(key=lambda y: int(y[0]))

    first = job_timestamps[0]
    last = job_timestamps[-1]

    #dt = datetime.datetime.fromtimestamp(os.path.getctime(base_time), datetime.timezone(datetime.timedelta(hours=0)))
    dt = datetime.datetime.fromtimestamp(os.path.getctime(base_time))

#    if execution:
#        queue_times = []
#        execution_times = []
#        for j in jobs:
#            mrsl_dict = load(os.path.join(mrsl_dir, j))#
#
#            queue_times.append(time.mktime(mrsl_dict['EXECUTING_TIMESTAMP']) - time.mktime(mrsl_dict['QUEUED_TIMESTAMP']))
#            execution_times.append(time.mktime(mrsl_dict['FINISHED_TIMESTAMP']) - time.mktime(mrsl_dict['EXECUTING_TIMESTAMP']))
    pathlib.Path(os.path.dirname(file_out)).mkdir(parents=True, exist_ok=True)
    with open(file_out, 'w') as f_out:
        f_out.write("Job count: "+ str(len(jobs)) +"\n")
        f_out.write("Generation time: "+ str(round(gen_time, 5)) +"\n")
        f_out.write("First trigger: "+ str(dt) +"\n")
        f_out.write("First scheduling datetime: "+ str(first[1]) +"\n")
        f_out.write("Last scheduling datetime: "+ str(last[1]) +"\n")
        f_out.write("First scheduling unixtime: "+ str(first[0]) +"\n")
        f_out.write("First scheduling unixtime: "+ str(last[0]) +"\n")
        f_out.write("Scheduling difference (seconds): "+ str(round(last[0] - first[0], 3)) +"\n")
        f_out.write("Initial scheduling delay (seconds): "+ str(round(first[0] - os.path.getctime(base_time), 3)) +"\n")
        total_time = round(last[0] - os.path.getctime(base_time), 3)
        f_out.write("Total scheduling delay (seconds): "+ str(total_time) +"\n")

#        if execution:
#            f_out.write("Average execution time (seconds): "+ str(round(mean(execution_times), 3)) +"\n")
#            f_out.write("Max execution time (seconds): "+ str(round(max(execution_times), 3)) +"\n")
#            f_out.write("Min execution time (seconds): "+ str(round(min(execution_times), 3)) +"\n")

#            f_out.write("Average queueing delay (seconds): "+ str(round(mean(queue_times), 3)) +"\n")
#            f_out.write("Max queueing delay (seconds): "+ str(round(max(queue_times), 3)) +"\n")
#            f_out.write("Min queueing delay (seconds): "+ str(round(min(queue_times), 3)) +"\n")

#            queue_times.remove(max(queue_times))
#            f_out.write("Average excluded queueing delay (seconds): "+ str(round(mean(queue_times), 3)) +"\n")

    return total_time

def mean(l):
    return sum(l)/len(l)

def collate_results(base_results_dir):

    scheduling_delays = []

    for run in os.listdir(base_results_dir):
        if run != 'results.txt':
            with open(os.path.join(base_results_dir, run, 'results.txt'), 'r') as f:
                d = f.readlines()

                for l in d:
                    if "Total scheduling delay (seconds): " in l:
                        scheduling_delays.append(float(l.replace("Total scheduling delay (seconds): ", '')))

    with open(os.path.join(base_results_dir, 'results.txt'), 'w') as f:
        f.write(f"Average schedule time: {round(mean(scheduling_delays), 3)}\n")
        f.write(f"Scheduling times: {scheduling_delays}")

def rmtree(directory):
    for root, dirs, files in os.walk(directory, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for dir in dirs:
            rmtree(os.path.join(root, dir))
    os.rmdir(directory)

def run_test(patterns, recipes, files_count, expected_job_count, repeats, job_counter, requested_jobs, runtime_start, signature='', execution=False, print_logging=False):
    if not os.path.exists(results_dir):
        os.mkdir(results_dir)

    if execution:
        os.system("export LC_ALL=C.UTF-8")
        os.system("export LANG=C.UTF-8")

    for run in range(repeats):
        # Ensure complete cleanup from previous run
        for f in [".workflow_processing", "job_output", "test_vgrid"]:
            if os.path.exists(f):
                print(f'path {f} exists')
                rmtree(f)
                print(f'path {f} deleted')

        base_dir = VGRID
        file_base = os.path.join(base_dir, 'testing')
        pathlib.Path(file_base).mkdir(parents=True, exist_ok=True)

        num_workers = 0
        if execution:
            num_workers = 1

        runner = meow.WorkflowRunner(
            VGRID,
            num_workers,
            patterns=patterns,
            recipes=recipes,
            daemon=True,
            start_workers=False,
            retro_active_jobs=False,
            print_logging=print_logging,
            file_logging=False,
            wait_time=1
        )

        # Let setup complete
        setting_up = 1
        setup_count = 0
        sleepy_time = 1
        if execution:
            runner.start_workers()

        while setting_up:
            time.sleep(sleepy_time)

            runner_patterns = runner.check_patterns()
            runner_recipes = runner.check_recipes()

            missing_patterns = [p for p in patterns.keys() if p not in runner_patterns.keys()]
            missing_recipes = [r for r in recipes.keys() if r not in runner_recipes.keys()]

            if not missing_recipes and not missing_patterns:
                setting_up = 0
            else:
                print(f"{str(setup_count)}: Missing patterns({str(len(missing_patterns))}) or recipes({str(len(missing_recipes))})")
                # Overloading watchdog give it more time to work out whats what
                sleepy_time = 5
                for p in missing_patterns:
                    runner.add_pattern(patterns[p])        
                for r in missing_recipes:
                    runner.add_recipe(recipes[r])
            setup_count+=1

        # Generate triggering files
        first_filename, generation_duration = generate(files_count, file_base +"/file_")

        getting_jobs = 1
        miss_count = 0
        previous_job_count = -1
        if execution:
            miss_target = 30
            total_jobs_found = 0
            while getting_jobs:
                time.sleep(1)
                jobs = runner.check_queue()
                print(f'Jobs: {str(previous_job_count)} {str(len(jobs))} {str(total_jobs_found)}')
                if previous_job_count == len(jobs):
                    miss_count+=1
                    if miss_count == miss_target:
                        getting_jobs = 0
                else:
                    if previous_job_count < len(jobs):
                        total_jobs_found += len(jobs)
                    miss_count = 0
                previous_job_count = len(jobs)
        
            print(f'Job queue settled at: {str(total_jobs_found)}')

            jobs = runner.get_all_jobs()

        else:
            miss_target = 10
            while getting_jobs:
                time.sleep(1)
                jobs = runner.check_queue()
                print(f'Jobs: {str(previous_job_count)} {str(len(jobs))}')
                if previous_job_count == len(jobs):
                    miss_count+=1
                    if miss_count == miss_target:
                        getting_jobs = 0
                else:
                    miss_count = 0
                previous_job_count = len(jobs)

            print(f'Job queue settled at: {str(len(jobs))}')

        results_path = os.path.join(results_dir, signature, str(expected_job_count), str(run), 'results.txt')

        cleanup(jobs, results_path, first_filename, generation_duration, execution=execution)

        # Leave some sleeps here, as can take a wee bit in the background
        time.sleep(3)
        runner.stop_runner(clear_jobs=True)
        time.sleep(3)

        print(f"Completed scheduling run {str(run + 1)} of {str(len(jobs))}/{str(expected_job_count)} jobs for '{signature}' {job_counter + expected_job_count*(run+1)}/{requested_jobs} ({str(round(time.time()-runtime_start, 3))}s)")

    collate_results(os.path.join(results_dir, signature, str(expected_job_count)))

def run_tests():
    start=100
    stop=1000
    jump=100
    repeats=10

    tests_to_run = [
        SINGLE_PATTERN_MULTIPLE_FILES,
        MULTIPLE_PATTERNS_SINGLE_FILE,
        SINGLE_PATTERN_SINGLE_FILE_PARALLEL,
        # These tests take ages, run them over a weeked
        MULTIPLE_PATTERNS_MULTIPLE_FILES,
        SINGLE_PATTERN_SINGLE_FILE_SEQUENTIAL
    ]

    requested_jobs=0
    jobs_count = start
    while jobs_count <= stop:
        requested_jobs += jobs_count * repeats * len(tests_to_run)
        jobs_count += jump
    print(f"requested_jobs: {requested_jobs}")

    runtime_start=time.time()
  
    job_counter=0
    jobs_count = start
    while jobs_count <= stop:

        print('starting')

        if SINGLE_PATTERN_MULTIPLE_FILES in tests_to_run:
            single_boring_pattern = meow.Pattern('pattern_one')
            single_boring_pattern.add_single_input('input', 'testing/*')
            single_boring_pattern.add_recipe('recipe_one')

            patterns = {
                single_boring_pattern.name: single_boring_pattern
            }

            single_recipe = meow.register_recipe('test.ipynb', 'recipe_one')
            
            recipes = {
                single_recipe['name']: single_recipe
            }

            run_test(
                patterns, 
                recipes, 
                jobs_count, 
                jobs_count,
                repeats, 
                job_counter,
                requested_jobs,
                runtime_start,
                signature=SINGLE_PATTERN_MULTIPLE_FILES
            )

            job_counter += jobs_count * repeats

        if MULTIPLE_PATTERNS_SINGLE_FILE in tests_to_run:
            patterns = {}
            for i in range(jobs_count):
                pattern = meow.Pattern(f"pattern_{i}")
                pattern.add_single_input('input', 'testing/*')
                pattern.add_recipe('recipe_one')
                patterns[pattern.name] = pattern

            run_test(
                patterns, 
                recipes, 
                1, 
                jobs_count,
                repeats, 
                job_counter,
                requested_jobs,
                runtime_start,
                signature=MULTIPLE_PATTERNS_SINGLE_FILE
            )

            job_counter += jobs_count * repeats

        if SINGLE_PATTERN_SINGLE_FILE_PARALLEL in tests_to_run:
            single_exciting_pattern = meow.Pattern('pattern_one')
            single_exciting_pattern.add_single_input('input', f'testing/*')
            single_exciting_pattern.add_recipe('recipe_one')
            single_exciting_pattern.add_param_sweep('var', {'increment': 1, 'start': 1, 'stop': jobs_count})
            patterns = {single_exciting_pattern.name: single_exciting_pattern}

            run_test(
                patterns, 
                recipes, 
                1, 
                jobs_count,
                repeats, 
                job_counter,
                requested_jobs,
                runtime_start,
                signature=SINGLE_PATTERN_SINGLE_FILE_PARALLEL
            )

            job_counter += jobs_count * repeats

        if MULTIPLE_PATTERNS_MULTIPLE_FILES in tests_to_run:
            patterns = {}
            for i in range(jobs_count):
                pattern = meow.Pattern(f"pattern_{i}")
                pattern.add_single_input('input', f'testing/file_{i}.txt')
                pattern.add_recipe('recipe_one')
                patterns[pattern.name] = pattern

            run_test(
                patterns, 
                recipes, 
                jobs_count, 
                jobs_count,
                repeats, 
                job_counter,
                requested_jobs,
            runtime_start,
                signature=MULTIPLE_PATTERNS_MULTIPLE_FILES
            )

            job_counter += jobs_count * repeats

        if SINGLE_PATTERN_SINGLE_FILE_SEQUENTIAL in tests_to_run:
            single_repeating_pattern = meow.Pattern('pattern_one')
            single_repeating_pattern.add_single_input('INPUT_FILE', f'testing/*')
            single_repeating_pattern.add_recipe('recipe_two')
            single_repeating_pattern.add_variable('MAX_COUNT', jobs_count)

            patterns = {
                single_repeating_pattern.name: single_repeating_pattern
            }

            repeating_recipe = meow.register_recipe('sequential.ipynb', 'recipe_two')
            
            recipes = {
                repeating_recipe['name']: repeating_recipe
            }

            run_test(
                patterns, 
                recipes, 
                1, 
                jobs_count,
                repeats, 
                job_counter,
                requested_jobs,
                runtime_start,
                signature=SINGLE_PATTERN_SINGLE_FILE_SEQUENTIAL,
                execution=True,
                print_logging=True
            )

            job_counter += jobs_count * repeats

        jobs_count += jump


if __name__ == '__main__':
    try:
        run_tests()
    except KeyboardInterrupt as ki:
        try:
            sys.exit(1)
        except SystemExit:
            os._exit(1)