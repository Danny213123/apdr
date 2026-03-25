import maya.cmds as cmds
import maya.mel as mel
import pymel.core as pm

cmds.polySphere(name="mySphere", radius=5)
mel.eval('sphere -r 3')
selected = pm.ls(selection=True)
