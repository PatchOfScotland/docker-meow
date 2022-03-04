from cProfile import label
import os
from turtle import pos, position, width
import numpy

import matplotlib.pyplot as pyplot

RESULTS_FOLDER = "results"
SCHEDULE_TEXT = 'Average schedule time: '

if __name__ == '__main__':
    scheduling_results = {}

    for run_type in os.listdir(RESULTS_FOLDER):
        scheduling_results[run_type] = []
        run_type_path = os.path.join(RESULTS_FOLDER, run_type)

        for job_count in os.listdir(run_type_path):
            results_path = os.path.join(run_type_path, job_count, 'results.txt')
            with open(results_path, 'r') as f_in:
                data = f_in.readlines()

            scheduling_duration = 0
            for line in data:
                if SCHEDULE_TEXT in line:
                    scheduling_duration = float(line.replace(SCHEDULE_TEXT, ''))

            scheduling_results[run_type].append((job_count, scheduling_duration))

            scheduling_results[run_type].sort(key=lambda y: float(y[0]))
    
    print(scheduling_results)

    pyplot.figure(figsize=(12, 6), dpi=250)
    for run_type in os.listdir(RESULTS_FOLDER):
        scheduling_x = numpy.asarray([float(i[0]) for i in scheduling_results[run_type]])
        scheduling_y = numpy.asarray([float(i[1]) for i in scheduling_results[run_type]])

        pyplot.plot(scheduling_x, scheduling_y, label=f'scheduling {run_type}')

    pyplot.xlabel("Amount of jobs scheduled")
    pyplot.ylabel("Time taken (seconds)")
    pyplot.title("mig_meow sheduling overheads")

    handles, labels = pyplot.gca().get_legend_handles_labels()
    legend_order = [2, 4, 0, 1, 3]
    pyplot.legend([handles[i] for i in legend_order], [labels[i] for i in legend_order])
    pyplot.yscale('log')
    pyplot.savefig("mig_meow_overheads.pdf", format='pdf', dpi=250, width=100, height=10)

