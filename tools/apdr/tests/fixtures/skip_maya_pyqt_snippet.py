from PyQt4 import QtCore
import maya.OpenMayaUI as mui
import sip

pointer = mui.MQtUtil.mainWindow()
wrapped = sip.wrapinstance(long(pointer), QtCore.QObject)
