import os

data_root = '/home/dan/data/run_1/task_1'

batterID = ''
gameYear = ''
leagueLevel = ''
f = open('/home/dan/tmp.txt', 'r')

data_f = open(data_root + os.sep + 'source_data.txt')
mft_f = open(data_root + os.sep + 'output.mft', 'w')

data_f.readline()

for line in data_f:
    row = line.split('\t')
    if batterID != row[2] or gameYear != row[6] or leagueLevel != row[4]:
        batterID = row[2]
        gameYear = row[6]
        leagueLevel = row[4]
        f.close()
        filename = data_root + os.sep + str(batterID) + '_' + str(gameYear) + '_' + leagueLevel + '.txt'
        mft_f.write(filename + '\n')
        f = open(filename, 'w')
        # create new file
    
    f.write(line)

data_f.close()
mft_f.close()
