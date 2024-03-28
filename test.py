from concurrent.futures import as_completed, ThreadPoolExecutor
import time
import queue

q = queue.Queue(maxsize=10)
def do_task(i):
    time.sleep(1)
    print(f"task({i})")

tasks = [x for x in range(100)]

if __name__ == "__main__":
        with ThreadPoolExecutor(max_workers=1) as exector:
            for x in range(100):

                print(f"range:{x}")
                exector.submit(do_task, x)




