#!/usr/bin/env python3
import os
import subprocess
import random

FIRST = [
    "Alice","Bob","Charlie","Diana","Eve","Frank","Grace","Henry","Ivy","Jack",
    "Karen","Leo","Mia","Nathan","Olivia","Paul","Quinn","Rachel","Sam","Tara",
    "Uma","Victor","Wendy","Xander","Yara","Zoe","Aaron","Beth","Carl","Donna",
    "Ethan","Fiona","George","Hannah","Igor","Julia","Kevin","Laura","Mike","Nina",
    "Oscar","Penny","Ray","Sara","Tom","Uma","Val","Will","Xena","Yusuf",
]

LAST = [
    "Smith","Jones","Brown","Taylor","Wilson","Davis","Evans","White","Green","Hall",
    "Wood","Thomas","Moore","Martin","Lee","Lewis","Clark","King","Baker","Scott",
    "Morris","Turner","Parker","Hill","Cooper","Ward","Collins","Reed","Cook","Bell",
    "Murphy","Bailey","Rivera","Cox","Howard","Price","Foster","Brooks","Ross","Cruz",
]

random.seed(41)
names = set()
while len(names) < 1000:
    name = random.choice(FIRST) + random.choice(LAST)
    if len(name) <= 32:
        names.add(name)

print("Generated names:", len(names))

cmds = ["TABLE people(name string PRIMARY KEY, age number)"]
#cmds = []
for name in sorted(names):
    age = random.randint(18, 80)
    cmds.append(f"NEW(people, '{name}', {age})")

cmds += ["COUNT(people)", "CHAINS(people)"]
input_text = "\n".join(cmds) + "\n"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(SCRIPT_DIR, "data")

result = subprocess.run(
    [os.path.join(SCRIPT_DIR, "build", "mql"), DATA_DIR],
    input=input_text,
    text=True,
    capture_output=True,
)
print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)
