"""Kivy + pyjnius Android app — requires display server and JVM.
Cannot run in headless Docker."""
from kivy.app import App
from kivy.lang import Builder
from jnius import autoclass

PythonActivity = autoclass('org.kivy.android.PythonActivity')

class MyApp(App):
    def build(self):
        return Builder.load_string('Button:\\n    text: "Hello"')
