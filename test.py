import Utilities

percorso="file:///Users/mac/file1.csv"

path1 = Utilities.filePathInProcessed(percorso)
path2 = Utilities.filePath(percorso)
path3 = Utilities.filePathFonte(percorso)
path4 = Utilities.filePathInProcessedNew(percorso)

print(path1)
print(path2)
print(path3)
print(path4)