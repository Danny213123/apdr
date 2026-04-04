#!/usr/bin/env python
"""LEGO EV3 hardware-specific snippet — ev3dev only works on EV3 brick."""
import evdev
import ev3dev.auto as ev3
import threading
import time

motor = ev3.LargeMotor('outA')
sensor = ev3.TouchSensor()
