# Databricks notebook source
import os
import sys
import math

# COMMAND ----------

# Configuring additional paths to find imports
sys.path.insert(1, r'C:\ODAOfficial\SDK_23.5_vc16\exe\vc16_amd64dll')
sys.path.insert(1, r'C:\ODAOfficial\SDK_23.5_vc16\\activation')
filename = "77c3ee08c5d540ec9bb899b284df1a8f.dwg"
file = "C:\ODAOfficial\Luna\EBO\dataInput\DWG\\" + filename

# COMMAND ----------

from ODA_PyMemManager_Py3 import *
from ODA_Kernel_Py3 import *
from ODA_Drawings_Py3 import *
from OdActivationInfo import *

# COMMAND ----------

class SystemServices(RxSystemServicesImpl):
    def __init__(self):
        RxSystemServicesImpl.__init__(self)
        ODA_Kernel_Py3.odActivate(ActivationData.userInfo, ActivationData.userSignature)

# COMMAND ----------

class UserApplicationServices(ExHostAppServices):
    def __init__(self):
        ExHostAppServices.__init__(self)

# COMMAND ----------

trCore = ODA_PyMemoryManager_Get_MemoryManager().StartTransaction()

# COMMAND ----------

systemServices = SystemServices()

odInitialize(systemServices)

hostApp = UserApplicationServices()
hostApp.disableOutput(True)

database = hostApp.readFile(file, True, False, ODA_Kernel_Py3.kShareDenyNo, "")

# COMMAND ----------

drawing_database = dict()

# COMMAND ----------

pModelSpace = database.getModelSpaceId().safeOpenObject()

# COMMAND ----------

pBlkIter = pModelSpace.newIterator()

# COMMAND ----------

pBlkIter.start()
while(not pBlkIter.done()):

    tr = ODA_PyMemoryManager_Get_MemoryManager().StartTransaction()

    pBlock = pBlkIter.objectId().safeOpenObject() # AcDbBlockReference
    objectId = pBlock.blockTableRecord().safeOpenObject() #AcDbBlockTableRecord

    objId = objectId.getName()

    extents = OdGeExtents3d()

    if (eOk == objectId.getGeomExtents(extents)):
        minX = extents.minPoint().x
        minY = extents.minPoint().y
        minZ = extents.minPoint().z
        maxX = extents.maxPoint().x
        maxY = extents.maxPoint().y
        maxZ = extents.maxPoint().z

        pEntIter = objectId.newIterator()
        pEntIter.start()

        while(not pEntIter.done()):
            tr_inner = ODA_PyMemoryManager_Get_MemoryManager().StartTransaction()
            res = pEntIter.objectId().safeOpenObject()
            pEnt = pEntIter.objectId().safeOpenObject()
            rotation = 0
            if(res == eOk):
                pEnt = OdDbEntity_cast(pEnt);
                if (pEnt != None):
                    rotation = pEnt.rotation()
            rotationRad = rotation
            rotationDeg = math.degrees(rotation)
            ODA_PyMemoryManager_Get_MemoryManager().StopTransaction(tr_inner)
            pEntIter.step()

        drawing_database[objId] = {
            'minX' : minX, 
            'minY' : minY,
            'minZ' : minZ,
            'maxX' : maxX,
            'maxY' : maxY,
            'maxZ' : maxZ,
            'rotRad' : rotationRad,
            'rotDeg' : rotationDeg
            }

        pBlkIter.step()
    else:
        pBlkIter.step()

ODA_PyMemoryManager_Get_MemoryManager().StopTransaction(tr)

# COMMAND ----------

import csv

# COMMAND ----------

dwgFileName = filename.rsplit(".", 1)[0]

# COMMAND ----------

# Make CSV
columns = "ObjectId,Min Extents X,Min Extents Y,Min Extents Z,Max Extents X,Max Extents Y,Max Extents Z,Rotation Degrees,Rotation Radians\n"

local_path = r"C:\ODAOfficial\Luna\EBO\dataOutput\notebookTest\\"
if not os.path.exists(local_path):
    os.makedirs(local_path)

csv_filename = local_path + dwgFileName + ".csv"

# COMMAND ----------

with open(csv_filename, 'w', newline='') as csvfile:
    csvfile.write(columns)


# COMMAND ----------

for key, value in drawing_database.items():
    v_minX = drawing_database[key].get('minX')
    v_minY = drawing_database[key].get('minY')
    v_minZ = drawing_database[key].get('minZ')
    v_maxX = drawing_database[key].get('maxX')
    v_maxY = drawing_database[key].get('maxY')
    v_maxZ = drawing_database[key].get('maxZ')
    v_rotRad = drawing_database[key].get('rotRad')
    v_rotDeg = drawing_database[key].get('rotDeg')
    with open(csv_filename, 'a', newline='') as csvfile:
        fileWriter = csv.writer(csvfile)
        fileWriter.writerow([key, v_minX, v_minY, v_minZ, v_maxX, v_maxY, v_maxZ, v_rotRad, v_rotDeg])
