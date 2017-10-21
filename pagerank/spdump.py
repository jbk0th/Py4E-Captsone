import sqlite3

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

# Likely things not done in the exact order, tables are joined
# THEN with new super table, all of the from_ids are counted b/c were joined on the to_id, with the Pages info
# probably many repeats of the same info except the from_id, then
# the group by takes care of all the multiple entries, think of it like a pivot tables
###
cur.execute('''SELECT COUNT(from_id) AS inbound, old_rank, new_rank, id, url
     FROM Pages JOIN Links ON Pages.id = Links.to_id
     WHERE html IS NOT NULL
     GROUP BY id ORDER BY inbound DESC''')

count = 0
for row in cur :
    if count < 50 : print(row)
    count = count + 1
print(count, 'rows.')
cur.close()
