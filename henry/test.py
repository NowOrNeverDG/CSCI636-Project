with open("../dataset/ml-100k/u.item", mode="r", encoding="ISO-8859-1") as reader:
  for line in reader:
    spliter = line.split("|")
    print(len(spliter))