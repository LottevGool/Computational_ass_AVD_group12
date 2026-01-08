from collections import deque
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

# Helper functions
def converter_simulationtime_clocktime(simulation_time):
    day = int(simulation_time // workday)
    minutes_elapsed_since_8 = simulation_time % workday
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
            "day": appointment_day,
            "start_scheduled": start_scheduled,
            "finish_scheduled": finish_scheduled,
            "start_true": start_true,
            "finish_true": finish_true,
        }


# Each patient is SimPy process
def patient(environ, scheduler, data, patient_ID):
    arrival_time = data["arrival_time"]
    call_day, call_time = converter_simulationtime_clocktime(arrival_time)

    # Store when the patient has called for an appointment
    yield environ.timeout(arrival_time - environ.now)
    print(f"Day {call_day}: Patient {patient_ID} calls at {call_time}")

    # Schedule patient after they've called
    result = scheduler.assign(data)

    day = result["day"]
    machine = result["machine"]
    start_scheduled = result["start_scheduled"]
    finish_scheduled = result["finish_scheduled"]

    # Scheduled slot
    start_scheduled_day, start_scheduled_time = converter_simulationtime_clocktime(start_scheduled)
    finish_scheduled_day, finish_scheduled_time = converter_simulationtime_clocktime(finish_scheduled)
    print(f"Day {day}: patient {patient_ID} scheduled on {machine} in slot: {start_scheduled_time} to {finish_scheduled_time}")

    # Check if there are any delays in the schedule by reporting the true start and finish
    # start and finish times reset each day but the simulation doesnt distinguish days and keeps counting

    # Actual start time of the scan
    total_start = day * workday + result["start_true"]
    start_true_day, start_true_time = converter_simulationtime_clocktime(total_start)
    yield environ.timeout(total_start - environ.now)
    print(f"Day {day}: patient {patient_ID} starts scan at {start_true_time} on {machine}")

    # Actual time the scan finishes
    total_finish = day * workday + result["finish_true"]
    finish_true_day, finish_true_time = converter_simulationtime_clocktime(total_finish)
    yield environ.timeout(total_finish - environ.now)
    print(f"Day {day}: patient {patient_ID} finishes scan at {finish_true_time} on {machine}")

# Calculate overtime per machine in minutes
def overtime_computer(machine):
    daily_overtime = {}
    for day, finish in machine.daily_finish_time.items():
        daily_overtime[day] = max(0, finish - workday)
    return daily_overtime

# Run simulation
def run_simulation(system_type):
    environ = simpy.Environment()
    scheduler = Scheduler(system_type)

    for patient_ID, d in enumerate(data):
        environ.process(patient(environ, scheduler, d, patient_ID))

    environ.run()

    # Overtime calculation
    if system_type == "old":
        machines = [scheduler.MRI_type1, scheduler.MRI_type2]
    else:
        machines = scheduler.machines

    print(f"Overtime Computation:")
    for m in machines:
        daily_overtime = overtime_computer(m)
        total_overtime = sum(daily_overtime.values())

        print(f"{m.name}:")
        for day, overtime in daily_overtime.items():
            print(f"Overtime on day {day}: {overtime:.0f} minutes")
        print(f"Total overtime: {total_overtime:.0f} minutes\n")

# Choose what system to use: "old" or "new"
run_simulation("old")

