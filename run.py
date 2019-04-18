from Normal import Normal
from Anomalous import Anomalous

NUM_CPU = 4
DATASET = 1  # 'Normal': 0, 'Anomalous': 1
SUBSET = True
SUBSET_SIZE = 30000

'''
Put the paths of files you wanna process into the list.
'''
PATH = ['/home/yinzheng/Documents/original csv/Friday_Botnet_ARES.csv',
        '/home/yinzheng/Documents/original csv/Friday_Port_Scan_DDos.csv',
        '/home/yinzheng/Documents/original csv/Thursday_Cool_MAC.csv',
        '/home/yinzheng/Documents/original csv/Thursday_Inf_Dropbox.csv',
        '/home/yinzheng/Documents/original csv/Thursday_PortscanNmap.csv',
        '/home/yinzheng/Documents/original csv/Thursday_Web.csv',
        '/home/yinzheng/Documents/original csv/Wednesday_DoS.csv',
        '/home/yinzheng/Documents/original csv/Wednesday_Heartbleed.csv',
        '/home/yinzheng/Documents/original csv/Tuesday_BruteForce.csv']

PATH_OUT = '/home/yinzheng/Documents/original csv/dataset_30000.csv'

'''
DO NOT edit anything below!
'''

if DATASET == 0:
    norm = Normal(PATH, PATH_OUT, NUM_CPU)
    norm.process_individual_data()
    norm.concatenate_all(subset=SUBSET, subset_size=SUBSET_SIZE)
elif DATASET == 1:
    anorm = Anomalous(PATH, NUM_CPU)
    anorm.process_individual_data()
else:
    print('Dataset hasn\'t correctly selected.')
