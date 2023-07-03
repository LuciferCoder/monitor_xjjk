
date_list = []
while True:
    try:
        m=input().split('-')
        date_list.append(m)
    except:
        break
print(date_list)