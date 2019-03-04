import sqlite3
import datetime

uname = '#'
conn = sqlite3.connect('/Users/{}/Library/Messages/chat.db'.format(uname,))
c = conn.cursor()
max_chat_id = c.execute('select max(chat_id) from chat_message_join').fetchone()[0]
start_epoch = 978307200  # imessage timestamps measured since 01/01/2001
one_billion = 1000000000  # imessage timestamps measured in billionths of seconds

sender_dict = {}
senders = c.execute('select ROWID, id from handle')
for sender in senders:
    sender_dict[sender[0]] = sender[1]

for i in range(1, max_chat_id + 1):
    chatlog = open('chatlog_id_{}.txt'.format(i,), 'w')
    query = '''select ROWID, text, is_from_me, handle_id, date
               from message t1
               inner join chat_message_join t2
               on t2.chat_id = {}
               and t1.ROWID = t2.message_id
               order by t1.date asc'''.format(i,)

    messages = c.execute(query)
    
    for message in messages:
        if not message[1]:
            text = ''
        else:
            text = message[1]
        is_from_me = message[2]
        handle_id = message[3]
        # truncate milliseconds to human readable date
        date = str(datetime.datetime.utcfromtimestamp(message[4] / one_billion + start_epoch))[:-7]

        # build messaage
        msg = ""
        msg += '[{}] '.format(date)
        if is_from_me == 1:
            msg += 'Me: '
        else:
            msg += '{}: '.format(sender_dict[handle_id])
        msg += text + '\n'
        chatlog.write(msg)

    chatlog.close()

c.close()
conn.close()