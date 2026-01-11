import os
import simpy
import csv

# File to read data from
filename = "ScanRecords.csv"

# Working directory
os.chdir("C:/Users/lotte/OneDrive/Documenten/Maastricht University/Year 4/Period 3/Computational Research Skills")

# Parameters (in minutes)
workday = 9 * 60
slot1 = 30
slot2 = 54

# Read data
data = []

with open(filename, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["PatientType"] == "Type 1":
            patient_type = 1
        elif row["PatientType"] == "Type 2":
            patient_type = 2

        data.append({
            "Date": row["Date"],
            "Time": float(row["Time"]),
            "Duration": float(row["Duration"]),
            "PatientType": patient_type
        })

# Convert dates to indices so they can be used more efficiently when scheduling
# e.g: 01-08-2025 -> 0, 02-08-2025 -> 1, etc.
converted_days = {}
index = 0

for d in data:
    if d["Date"] not in converted_days:
        converted_days[d["Date"]] = index
        index += 1
# Compute arrival times in minutes taking the note of the assignment into account:
# Note: when calculating arrival times, you may simply assume that no time elapsed outside of working
# hours. Hence, the time between a call at 16:45 om day 1 and 8:15 on day 2 is simply a half an hour.
for d in data:
    d["arrival_time"] = (converted_days[d["Date"]] * workday + d["Time"] * 60)

# Helper function
def converter_timeindex_clocktime(time_index):
    day = int(time_index // workday)
    minutes_elapsed_since_8 = time_index % workday
    hour = 8 + int(minutes_elapsed_since_8 // 60)
    minute = int(minutes_elapsed_since_8 % 60)
    return day, f"{hour:02d}:{minute:02d}"

# MRI Machine Class which keeps track when each machine is available
class MRIMachine:
    def __init__(self, name):
        self.name = name
        self.next_free_time = {}   # index = day, value = minutes elapsed since 08:00
        self.daily_finish_time = {} # index = day, value = true finish time of the last appointment
        self.scheduled_slots = {} # index = day, value = number of slots assigned to patients

    def slot_roster(self, day, slot_length):
        if day not in self.scheduled_slots:
            self.scheduled_slots[day] = 0
        slot_number = self.scheduled_slots[day]
        start_scheduled = slot_number * slot_length
        finish_scheduled = start_scheduled + slot_length
        self.scheduled_slots[day] += 1
        return start_scheduled, finish_scheduled

    def scanning(self, day, length_true, start_scheduled):
        # Start a new day and ensures no scans are split across days
        if day not in self.next_free_time:
            self.next_free_time[day] = 0
            self.daily_finish_time[day] = 0

        # A scan might take more time than is scheduled, resulting in a delay of the start of the next patients scans.
        start_true = max(start_scheduled, self.next_free_time[day])
        finish_true = start_true + length_true

        # Update machine availability for the next appointment
        self.next_free_time[day] = finish_true
        self.daily_finish_time[day] = max(self.daily_finish_time[day], finish_true)
        return start_true, finish_true

# Schedule patients to machines
class Scheduler:
    def __init__(self, system_type):
        self.system_type = system_type

        if system_type == "old":
            self.MRI_type1 = MRIMachine("MRI_type1")
            self.MRI_type2 = MRIMachine("MRI_type2")
        else:
            self.machines = [MRIMachine("MRI_1"), MRIMachine("MRI_2")]

    def assign(self, data):
        patient_type = data["PatientType"]
        arrival_time = data["arrival_time"]

        slot_length = slot1 if patient_type == 1 else slot2

        # Some patient may exceed the assigned slot length
        length_true = data["Duration"] * 60

        # Patients can at earliest be scheduled the next working day
        call_day = int(arrival_time // workday)
        appointment_day = call_day + 1  # no same-day scheduling

        if self.system_type == "old":
            # Assign to dedicated machine
            machine = self.MRI_type1 if patient_type == 1 else self.MRI_type2

        else:
            # Find the machine with earliest available slot and assign
            machine = self.machines[0]
            earliest_available = machine.next_free_time.get(appointment_day,0)

            for m in self.machines:
                free = m.next_free_time.get(appointment_day,0)
                if free < earliest_available:
                    earliest = free
                    machine = m

        # Slot assigned to patient
        start_scheduled ,finish_scheduled = machine.slot_roster(appointment_day, slot_length)

        # True slot the scan needs
        start_true, finish_true = machine.scanning(appointment_day, length_true, start_scheduled)

        return {
            "machine": machine.name,
            "patient_type": patient_type,
            "day": appointment_day,
            "arrival_time": arrival_time,
            "start_scheduled": start_scheduled,
            "finish_scheduled": finish_scheduled,
            "start_true": start_true,
            "finish_true": finish_true,
        }


# Each patient is SimPy process
def patient(environ, scheduler, data, patient_ID, simulation_results):
    arrival_time = data["arrival_time"]
    call_day, call_time = converter_timeindex_clocktime(arrival_time)

    # Store when the patient has called for an appointment
    yield environ.timeout(arrival_time - environ.now)
    # print(f"Day {call_day}: Patient {patient_ID} calls at {call_time}")

    # Schedule patient after they've called
    result = scheduler.assign(data)
    simulation_results.append(result)

    day = result["day"]
    machine = result["machine"]
    start_scheduled = result["start_scheduled"]
    finish_scheduled = result["finish_scheduled"]

    # Scheduled slot
    start_scheduled_day, start_scheduled_time = converter_timeindex_clocktime(start_scheduled)
    finish_scheduled_day, finish_scheduled_time = converter_timeindex_clocktime(finish_scheduled)
    # print(f"Day {day}: patient {patient_ID} scheduled on {machine} in slot: {start_scheduled_time} to {finish_scheduled_time}")

    # Check if there are any delays in the schedule by reporting the true start and finish
    # start and finish times reset each day but the simulation does not distinguish days and keeps counting
    # Hence, the time in day is converted to a "total time" in the simulation

    # Actual start time of the scan
    total_start = day * workday + result["start_true"]
    start_true_day, start_true_time = converter_timeindex_clocktime(total_start)
    yield environ.timeout(total_start - environ.now)
    # print(f"Day {day}: patient {patient_ID} starts scan at {start_true_time} on {machine}")

    # Actual time the scan finishes
    total_finish = day * workday + result["finish_true"]
    finish_true_day, finish_true_time = converter_timeindex_clocktime(total_finish)
    yield environ.timeout(total_finish - environ.now)
    # print(f"Day {day}: patient {patient_ID} finishes scan at {finish_true_time} on {machine}")

# Next, some functions to calculate KPIs are defined

# Number of patients of each type
def type_counter(data):
    type1_count = 0
    type2_count = 0
    for d in data:
        if d["PatientType"] == 1:
            type1_count += 1
        elif d["PatientType"] == 2:
            type2_count += 1
    return type1_count, type2_count

# Waiting time from call to start of appointment
# THRESHOLD IS IN WORKINGDAYS, i.e. a week consists of 5 workingdays
def waitingtime(simulation_results, threshold):
    patient_waiting_times = []
    significant_wait_count = 0

    # Instead of working hours we now also consider "non-working hours as the patient also has to wait then.
    for result in simulation_results:
        waiting_time = (result["start_scheduled"] + result["day"] * (24 * 60)) - result["arrival_time"]
        patient_waiting_times.append(waiting_time)
        if waiting_time > (threshold * (24*60)):
            significant_wait_count += 1

    mean = sum(patient_waiting_times) / len(patient_waiting_times)
    variance = sum((waiting_time - mean) ** 2 for waiting_time in patient_waiting_times) / len(patient_waiting_times)
    minimum = min(patient_waiting_times)
    maximum = max(patient_waiting_times)
    patient_wait = patient_waiting_times
    significant_wait = (significant_wait_count / len(patient_waiting_times)) * 100

    return {
        "mean": mean,
        "variance": variance,
        "minimum": minimum,
        "maximum": maximum,
        "waiting time per patient": patient_wait,
        "percentage of patients with waiting time above threshold": significant_wait
    }

# Downtime per machine / facility
def downtime(machine, simulation_results):
    daily_downtime = []

    for day in machine.scheduled_slots:
        total_scheduled = 0

        # Each machine is active as long as a patient is assigned to a slot
        # So, downtime of a machine is workday - assigned number of slots * slot_length
        appointments = [result for result in simulation_results
                    if result["machine"] == machine.name and result["day"] == day]
        total_scheduled = sum(result["finish_scheduled"] - result["start_scheduled"] for result in appointments)

        downtime = max(0, workday - total_scheduled)
        daily_downtime.append(downtime)

    mean = sum(daily_downtime) / len(daily_downtime)
    variance = sum((downtime - mean) ** 2 for downtime in daily_downtime) / len(daily_downtime)
    minimum = min(daily_downtime)
    maximum = max(daily_downtime)

    return {
        "mean": mean,
        "variance": variance,
        "minimum": minimum,
        "maximum": maximum,
        "downtime per day": daily_downtime
    }

# Throughput per day per machine / facility
def throughput(machine, simulation_results):
    daily_throughput = []

    for day in machine.scheduled_slots:
        appointments = [result for result in simulation_results
                    if result["machine"] == machine.name and result["day"] == day]
        throughput = len(appointments)
        daily_throughput.append(throughput)

    mean = sum(daily_throughput) / len(daily_throughput)
    variance = sum((throughput - mean) ** 2 for throughput in daily_throughput) / len(daily_throughput)
    minimum = min(daily_throughput)
    maximum = max(daily_throughput)

    return {
        "mean": mean,
        "variance": variance,
        "minimum": minimum,
        "maximum": maximum,
        "throughput per day": daily_throughput
    }

# Overtime per machine / facility
def overtime(machine):
    daily_overtime = []

    for day, finish in machine.daily_finish_time.items():
        overtime = max(0, finish - workday)
        daily_overtime.append(overtime)

    mean = sum(daily_overtime) / len(daily_overtime)
    variance = sum((overtime - mean) ** 2 for overtime in daily_overtime) / len(daily_overtime)
    minimum = min(daily_overtime)
    maximum = max(daily_overtime)

    return {
        "mean": mean,
        "variance": variance,
        "minimum": minimum,
        "maximum": maximum,
        "overtime per day": daily_overtime
    }

# Delay from the start of an appointment to when the patient is actually seen
# THRESHOLD IS IN MINUTES
def delay(simulation_results, threshold):
    patient_delays = []
    delay_count = 0
    total_delay_delayed = 0
    significant_delay_count = 0

    for result in simulation_results:
        delay = max(0, result["start_true"] - result["start_scheduled"])
        patient_delays.append(delay)
        if delay > 0:
            delay_count += 1
            total_delay_delayed += delay
            if delay > threshold:
                significant_delay_count += 1

    mean = sum(patient_delays) / len(patient_delays)
    variance = sum((delay - mean) ** 2 for delay in patient_delays) / len(patient_delays)
    minimum = min(patient_delays)
    maximum = max(patient_delays)
    delay_percentage = (delay_count / len(patient_delays)) * 100
    mean_delay_delayed = (total_delay_delayed / delay_count) * 100
    significant_delay = (significant_delay_count / len(patient_delays)) * 100

    return {
        "mean": mean,
        "variance": variance,
        "minimum": minimum,
        "maximum": maximum,
        "delays per day": patient_delays,
        "percentage of delayed patients": delay_percentage,
        "mean delay of all delayed patients": mean_delay_delayed,
        "percentage of patients with delay above threshold": significant_delay
    }

# Run simulation
def run_simulation(system_type):
    environ = simpy.Environment()
    scheduler = Scheduler(system_type)
    simulation_results = []

    for patient_ID, d in enumerate(data):
        environ.process(patient(environ, scheduler, d, patient_ID, simulation_results))

    environ.run()

    # KPI's corresponding to the schedule found in the simulation
    print(f"\n----- KPI REPORT for the {system} system -----")

    # Patient type
    type1_count, type2_count = type_counter(data)

    print("Number of patients of each type:")
    print(f" Type 1: {type1_count}")
    print(f" Type 2: {type2_count}\n")

    # Waiting time
    waitingtime_kpis = waitingtime(simulation_results, 5)           #n.b. threshold should be in WORKINGDAYS

    print(f"Waiting time from call to start of scan:")
    print(f"Average waiting time: {waitingtime_kpis['mean']:.1f} minutes")
    print(f"Variance in waiting time: {waitingtime_kpis['variance']:.1f}")
    print(f"Minimum waiting time: {waitingtime_kpis['minimum']:.1f} minutes")
    print(f"Maximum waiting: {waitingtime_kpis['maximum']:.1f} minutes")
    print(f"Percentage of patients with a waiting time above the threshold: {waitingtime_kpis['percentage of patients with waiting time above threshold']:.1f}%\n")


    if system_type == "old":
        machines = [scheduler.MRI_type1, scheduler.MRI_type2]
    else:
        machines = scheduler.machines

    # Downtime
    print(f"Downtime per Facility:")
    for m in machines:
        downtime_kpis = downtime(m, simulation_results)

        print(f"Facility {m.name}:")
        print(f"Average downtime: {downtime_kpis['mean']:.1f} minutes")
        print(f"Variance in downtime: {downtime_kpis['variance']:.1f}")
        print(f"Minimum downtime: {downtime_kpis['minimum']:.1f} minutes")
        print(f"Maximum downtime: {downtime_kpis['maximum']:.1f} minutes \n")

    # Throughput
    print(f"Throughput per Facility:")
    for m in machines:
        throughput_kpis = throughput(m, simulation_results)

        print(f"Facility {m.name}:")
        print(f"Average throughput: {throughput_kpis['mean']:.1f} patients")
        print(f"Variance in throughput: {throughput_kpis['variance']:.1f}")
        print(f"Minimum throughput: {throughput_kpis['minimum']:.0f} patients")
        print(f"Maximum throughput: {throughput_kpis['maximum']:.0f} patients \n")

    # Overtime
    print(f"Overtime per Facility:")
    for m in machines:
        overtime_kpis = overtime(m)

        print(f"Facility {m.name}:")
        print(f"Average overtime: {overtime_kpis['mean']:.1f} minutes")
        print(f"Variance in overtime: {overtime_kpis['variance']:.1f}")
        print(f"Minimum overtime: {overtime_kpis['minimum']:.1f} minutes")
        print(f"Maximum overtime: {overtime_kpis['maximum']:.1f} minutes \n")

    # Delay
    delay_kpis = delay(simulation_results, 60)          #n.b. threshold should be in MINUTES

    print(f"Delay from scheduled to the true start of the scan:")
    print(f"Average delay: {delay_kpis['mean']:.1f} minutes")
    print(f"Variance in delay: {delay_kpis['variance']:.1f}")
    print(f"Minimum delay: {delay_kpis['minimum']:.1f} minutes")
    print(f"Maximum delay: {delay_kpis['maximum']:.1f} minutes")
    print(f"Percentage of delayed patients: {delay_kpis['percentage of delayed patients']:.1f}%")
    print(f"Average delay of delayed patients: {delay_kpis['mean delay of all delayed patients']:.1f} minutes")
    print(f"Percentage of patients with a delay above the threshold: {delay_kpis['percentage of patients with delay above threshold']:.1f}%\n")

systems = ["old", "new"]
for system in systems:
    run_simulation(system)
