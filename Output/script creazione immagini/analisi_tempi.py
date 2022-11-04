# -*- coding: utf-8 -*-
"""analisi tempi.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1yZ1RMf-Elveq_CRrfwp_-lnNkG3LCBJ2
"""

# importing the required library
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
 
# read a titanic.csv file
# from seaborn library
df = sns.load_dataset('titanic')
print(df.head())

t="Query CMS"


data_spark={'query':['opere_sottocartelle','opere_sottocartelle','opere_sottocartelle','opere','opere','opere','get_visite','get_visite','get_visite','get_visite_and_write','get_visite_and_write','get_visite_and_write']
      ,'input':['Input 1','Input 2','Input 3','Input 1','Input 2','Input 3','Input 1','Input 2','Input 3','Input 1','Input 2','Input 3']
      ,'tempo':[3.1202,3.4044,4.5802,1.7408,2.0741,3.1180,0.5854,0.5663,1.1035,3.9016,5.4333,153.1805]}


data_spark2={'query':['opere_sottocartelle','opere_sottocartelle','opere_sottocartelle','opere','opere','opere','get_visite','get_visite','get_visite']
      ,'input':['Input 1','Input 2','Input 3','Input 1','Input 2','Input 3','Input 1','Input 2','Input 3']
      ,'tempo':[3.1202,3.4044,4.5802,1.7408,2.0741,3.1180,0.5854,0.5663,1.1035]}

data_cms={'query':['Q2','Q2','Q2','Q3','Q3','Q3','Q4','Q4','Q4','Q5','Q5','Q5','Q6','Q6','Q6']
      ,'input':['Input 1','Input 2','Input 3','Input 1','Input 2','Input 3','Input 1','Input 2','Input 3','Input 1','Input 2','Input 3','Input 1','Input 2','Input 3']
      ,'tempo':[0.001131,0.002339,0.001492,0.004366,0.024106,0.219407,0.004467,0.024837,0.177085,0.020891,1.851982,0,0.012458,0.080225,3.181484]}

df=pd.DataFrame(data_cms)
 
# who v/s fare barplot
sns.barplot(x = 'query',
            y = 'tempo',
            hue = 'input',
            data = df, log = True).set(title=t)
# Show the plot
plt.ylabel('tempo (log)')
plt.show()

# importing the required library
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
 
t="Q6"


data={'input':['Input 1','Input 2','Input 3'],'tempo':[1,2,3]}
opere_sottocartelle={'input':['Input 1','Input 2','Input 3'],'tempo':[3.1202,3.4044,4.5802]}
opere={'input':['Input 1','Input 2','Input 3'],'tempo':[1.7408,2.0741,3.1180]}
get_visite={'input':['Input 1','Input 2','Input 3'],'tempo':[0.5854,0.5663,1.1035]}
get_visite_and_write={'input':['Input 1','Input 2','Input 3'],'tempo':[3.9016,5.4333,153.1805]}

#CMS
getOpere={'input':['Input 1','Input 2','Input 3'],'tempo':[0.001131,0.002339,0.001492]}
getOpereSecolo={'input':['Input 1','Input 2','Input 3'],'tempo':[0.004366,0.024106,0.219407]}
getOperePerVisite={'input':['Input 1','Input 2','Input 3'],'tempo':[0.004467,0.024837,0.177085]}
getOpereSesso={'input':['Input 1','Input 2','Input 3'],'tempo':[0.020891,1.851982,0]}
creaPercorso={'input':['Input 1','Input 2','Input 3'],'tempo':[0.012458,0.080225,3.181484]}









df=pd.DataFrame(creaPercorso)
 
# who v/s fare barplot
sns.barplot(x = 'input',
            y = 'tempo',
            data = df, log=True).set(title=t)
plt.ylabel('tempo (log)')
# Show the plot
plt.show()