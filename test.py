# coding=utf-8
import sys

# from lbrynet.daemon.DaemonControl import start
# start()


from lbrynet.daemon.DaemonCLI import main
commands = list()
commands.append('wallet_balance')
commands.append('mzyENEG4gbiu5XgY2GtT6BzZWepAhCyyKJ')
# commands.append('True')
# commands.append(r'--file_path=D:\btcnano-wallet-client-desktop.zip')
sys.argv.extend(commands)
main()

