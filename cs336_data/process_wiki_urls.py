import sys

if __file__ == "__main__":
    file = sys.argv[1]
    with open(file, "r") as f:
        for line in f:
            url = line.strip()
            if not url:
                continue
            print(url)