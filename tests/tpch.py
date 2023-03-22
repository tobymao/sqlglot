import time

from sqlglot.optimizer import optimize

INPUT = ""
OUTPUT = ""
NUM = 99
SCHEMA = {}
KIND = "DS"

with open(OUTPUT, "w", encoding="UTF-8") as fixture:
    for i in range(NUM):
        i = i + 1
        with open(INPUT.format(i=i), encoding="UTF-8") as file:
            original = "\n".join(
                line.rstrip()
                for line in file.read().split(";")[0].split("\n")
                if not line.startswith("--")
            )
            original = original.replace("`", '"')
            now = time.time()
            try:
                optimized = optimize(original, schema=SCHEMA)
            except Exception as e:
                print("****", i, e, "****")
                continue

            fixture.write(
                f"""--------------------------------------
-- TPC-{KIND} {i}
--------------------------------------
{original};
{optimized.sql(pretty=True)};

"""
            )
            print(i, time.time() - now)
