from Normal import Normal
from Anomalous import Anomalous

NUM_CPU = 4
DATASET = 0  # 'Normal': 0, 'Anomalous': 1
SUBSET = True
SUBSET_SIZE = 20000

'''
Put the paths of files you wanna process into the list.
'''
PATH = ['/path_to/Monday_norm.csv',
        '/path_to/Tuesday_norm.csv',
        '/path_to/Wednesday_norm.csv',
        '/path_to/Thursday_norm.csv',
        '/path_to/Friday_norm.csv',
        ]

PATH_OUT = 'path_to/dataset_20000_test.csv'

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
