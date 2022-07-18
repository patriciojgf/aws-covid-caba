#!/usr/bin/env python
import subprocess

print("Ejecucion Clean Tables")
subprocess.call(['python3', 'clean_tables.py'])
print("Ejecucion Load ID Tables")
subprocess.call(['python3', 'id_tables.py'])
print("Ejecucion Load Vacunacion")
subprocess.call(['python3', 'vacunacion.py'])
print("Ejecucion Load Contagiados y Fallecidos")
subprocess.call(['python3', 'casos.py'])
print("Ejecucion Load Poblacion")
subprocess.call(['python3', 'poblacion.py'])