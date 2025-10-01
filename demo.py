
import time
from virtualshell import Shell, ExecutionResult

def example_concurrent_work():
    """
    Example showing how to do other work while waiting for async PowerShell execution.
    This demonstrates the power of execute_async - you can continue processing while
    PowerShell commands run in the background.
    """
    print("=== Concurrent Work Example ===")
    
    with Shell() as sh:
        # Set up a PowerShell function that takes some time to execute
        print("Setting up PowerShell function with 10 ms delay per increment...")
        sh.execute("function SlowInc { Start-Sleep -Milliseconds 10; $global:i += 1; $global:i }")
        sh.execute("$global:i = 0")
        
        # Results storage
        async_results = []
        work_done = []
        
        def async_callback(r: ExecutionResult) -> None:
            """Callback called when async execution completes"""
            print(f"✓ Async PowerShell completed! Final value: {r.output.strip()}")
            async_results.append(r.output.strip())
        
        # Start a long-running PowerShell command asynchronously
        print("Starting async PowerShell execution (1000 increments)...")
        to_run = "SlowInc;" * 1000
        future = sh.execute_async(to_run, callback=async_callback)
        
        print("Now doing other work while PowerShell runs in background..., max 50 iterations")
        
        # Do other work while PowerShell executes
        start_time = time.time()
        for i in range(50):
            # Simulate some CPU-intensive work
            result = sum(x*x for x in range(1000))
            work_done.append(f"Calculation {i+1}: sum of squares = {result}")
            
            # Check if async is done (non-blocking)
            if future.done():
                print(f"Async completed early at iteration {i+1}!")
                break
            
            # Small delay to make the example more visible
            time.sleep(0.1)
            
            if i % 10 == 0:
                print(f"  PowerShell is still working... completed {i+1} calculations to have some work done while waiting for PowerShell")
        
        # Wait for async to complete if it hasn't already
        if not future.done():
            print("Waiting for PowerShell to finish...")
            future.result()  # This will block until completion
        
        elapsed = time.time() - start_time
        
        print(f"\n=== Results ===")
        print(f"Total time: {elapsed:.2f} seconds")
        print(f"Calculations completed: {len(work_done)}")
        print(f"PowerShell final result: {async_results[0] if async_results else 'Not completed'}")
        print("✓ Successfully demonstrated concurrent execution!")

def example_multiple_async_tasks():
    """
    Example showing multiple async tasks running concurrently.
    """
    print("\n=== Multiple Async Tasks Example ===")
    
    with Shell(timeout_seconds=30) as sh:
        # Set up different PowerShell functions
        sh.execute("""
        function CountUp($max) { 
            for($i=1; $i -le $max; $i++) { 
                Start-Sleep -Milliseconds 5
            }
            return $i - 1
        }
        function CountDown($max) { 
            for($i=$max; $i -gt 0; $i--) { 
                Start-Sleep -Milliseconds 3
            }
            return $max - $i
        }
        """)
        
        results = {}
        
        def make_callback(task_name):
            def callback(r: ExecutionResult):
                results[task_name] = r.output.strip()
                print(f"✓ {task_name} completed: {r.output.strip()}")
            return callback
        
        # Start multiple async tasks
        print("Starting multiple async tasks...")
        future1 = sh.execute_async("CountUp 100", callback=make_callback("CountUp"))
        future2 = sh.execute_async("CountDown 150", callback=make_callback("CountDown"))
        
        # Do work while both run
        print("Doing work while both tasks run...")
        work_counter = 0
        while not (future1.done() and future2.done()):
            # Some work
            work_counter += 1
            time.sleep(0.05)
            if work_counter % 20 == 0:
                print(f"  Work units completed: {work_counter}")
        
        print(f"\n=== Multiple Tasks Results ===")
        print(f"Total work units: {work_counter}")
        for task, result in results.items():
            print(f"{task}: {result}")

def example_progress_monitoring():
    """
    Example showing batch execution with progress monitoring.
    """
    print("\n=== Progress Monitoring Example ===")
    
    with Shell(timeout_seconds=30) as sh:
        commands = [
            "Start-Sleep -Milliseconds 100; 'Task 1 done'",
            "Start-Sleep -Milliseconds 150; 'Task 2 done'", 
            "Start-Sleep -Milliseconds 200; 'Task 3 done'",
            "Start-Sleep -Milliseconds 120; 'Task 4 done'",
            "Start-Sleep -Milliseconds 180; 'Task 5 done'",
            "Start-Sleep -Milliseconds 130; 'Task 6 done'",
            "Start-Sleep -Milliseconds 170; 'Task 7 done'",
        ]
        
        progress_updates = []
        
        def progress_callback(progress):
            """
            Called for each command completion in batch.
            
            BatchProgress properties:
            - currentCommand: Index of the current command in the batch (1-based)
            - totalCommands: Total number of commands in the batch  
            - lastResult: ExecutionResult of the most recently completed command
            - isComplete: True when the batch has finished
            - allResults: List of all ExecutionResults (filled at completion)
            """
            msg = f"Progress: {progress.currentCommand}/{progress.totalCommands} - Last: {progress.lastResult.output.strip()}"
            progress_updates.append(msg)
            print(f"  {msg}")
            
            # You can also access other properties:
            # print(f"  Is complete: {progress.isComplete}")
            # if progress.isComplete:
            #     print(f"  All results count: {len(progress.allResults)}")
        
        print("Starting batch execution with progress monitoring...")
        future = sh.execute_async_batch(
            commands, 
            progress=progress_callback,
            stop_on_first_error=False
        )
        
        # Do other work while monitoring progress
        work_items = 0
        while not future.done():
            work_items += 1
            time.sleep(0.05)  # Simulate work
        
        results = future.result()
        print(f"\n=== Batch Results ===")
        print(f"Work items completed during batch: {work_items}")
        print(f"Total progress updates: {len(progress_updates)}")
        print(f"Batch commands completed: {len(results)}")

def example_python_vs_powershell_race(numbers_to_compute=20):
    """
    Race test between Python and PowerShell doing the same computational work.
    Whoever finishes first wins!
    """
    print("\n=== Python vs PowerShell Race Test ===")
    

    with Shell(timeout_seconds=240) as sh:
        # Set up PowerShell function for computation
        print("Setting up PowerShell computation function...")

        sh.execute("""
        function ComputeFactorials($max) {
            $sum = [System.Numerics.BigInteger]::Zero
            for($i = 1; $i -le $max; $i++) {
                $factorial = [System.Numerics.BigInteger]::One
                for($j = 1; $j -le $i; $j++) {
                    $factorial = [System.Numerics.BigInteger]::Multiply($factorial, $j)
                }
                $sum = [System.Numerics.BigInteger]::Add($sum, $factorial)
                # Small delay to make race more interesting
                Start-Sleep -Milliseconds 1
            }
            return $sum.ToString()
        }
        """)


        
        # Define the race parameters
        max_number = numbers_to_compute
        
        # Results storage
        python_result = None
        powershell_result = None
        python_finished = False
        powershell_finished = False
        race_winner = None
        
        def powershell_callback(r: ExecutionResult) -> None:
            nonlocal powershell_result, powershell_finished, race_winner
            powershell_result = int(r.output.strip())
            powershell_finished = True
            if not python_finished:
                race_winner = "PowerShell"
                print(f"🏆 PowerShell WINS! Result: {powershell_result}")
        
        print(f"🏁 Starting race: Computing sum of factorials from 1 to {max_number}")
        print("🐍 Python vs ⚡ PowerShell - May the fastest win!")
        
        # Start PowerShell computation
        powershell_command = f"ComputeFactorials {max_number}"
        future = sh.execute_async(powershell_command, callback=powershell_callback)
        
        # Start Python computation
        print("🚀 Race started!")
        start_time = time.time()
        
        python_sum = 0
        for i in range(1, max_number + 1):
            # Check if PowerShell finished first
            if future.done():
                race_winner = "PowerShell"
                print(f"🏆 PowerShell WINS! Python was only at number {i}")
                break
            
            # Calculate factorial using Python's built-in support for big integers
            factorial = 1
            for j in range(1, i + 1):
                factorial *= j
            python_sum += factorial
            
            # Small delay to match PowerShell timing
            time.sleep(0.001)
            
            if i <= 10 or i % 5 == 0:  # Show progress less frequently for large numbers
                print(f"  🐍 Python: Computing factorial {i}... (current sum: {python_sum})")
        else:
            # Python finished the loop
            python_result = python_sum
            python_finished = True
            if not powershell_finished:
                race_winner = "Python"
                print(f"🏆 Python WINS! Result: {python_result}")
        
        # Wait for both to complete if needed
        if not future.done():
            print("⏳ Waiting for PowerShell to finish...")
            future.result()
        
        race_time = time.time() - start_time
        
        # Get PowerShell execution time from the result
        powershell_exec_time = None
        if powershell_result is not None:
            # Get the actual ExecutionResult to access executionTime
            ps_result_obj = future.result()
            powershell_exec_time = ps_result_obj.execution_time
        
        # Calculate Python execution time (excluding waiting time)
        python_exec_time = None
        if python_finished:
            python_exec_time = race_time  # Python ran synchronously, so race_time is execution time
        
        print(f"\n=== Race Results ===")
        print(f"� Race Winner: {race_winner}")
        print(f"⏱️  Total race time: {race_time:.3f} seconds")
        print(f"🐍 Python result: {python_result if python_finished else 'Did not finish'}")
        print(f"⚡ PowerShell result: {powershell_result if powershell_finished else 'Did not finish'}")
        
        # Performance comparison using executionTime
        if python_exec_time is not None and powershell_exec_time is not None:
            print(f"\n=== Performance Analysis ===")
            print(f"🐍 Python execution time: {python_exec_time:.4f} seconds")
            print(f"⚡ PowerShell execution time: {powershell_exec_time:.4f} seconds")
            
            time_diff = abs(python_exec_time - powershell_exec_time)
            
            # Consider times equal if difference is less than 1ms
            if time_diff < 0.001:
                performance_winner = "Tie"
                print(f"🤝 Performance Tie: Both took virtually the same time! (diff: {time_diff:.4f}s)")
            elif python_exec_time < powershell_exec_time:
                performance_winner = "Python"
                print(f"🏆 Performance Winner: Python (faster by {time_diff:.4f} seconds)")
            else:
                performance_winner = "PowerShell"
                print(f"🏆 Performance Winner: PowerShell (faster by {time_diff:.4f} seconds)")
            
            # Compare race winner vs performance winner
            if race_winner != performance_winner and performance_winner != "Tie":
                print(f"🎭 Plot Twist: {race_winner} won the race, but {performance_winner} was actually faster!")
            elif race_winner == performance_winner:
                print(f"✨ Perfect Victory: {race_winner} won both the race and performance!")
            elif performance_winner == "Tie":
                print(f"⚖️  Fair Result: {race_winner} won the race, but performance was essentially tied!")
        
        # Verify results match (if both completed)
        if python_result is not None and powershell_result is not None:
            if python_result == powershell_result:
                print("✅ Results match - both computed correctly!")
            else:
                print("❌ Results don't match - something went wrong!")
        
        print(f"🎯 Challenge completed!")


if __name__ == "__main__":
    # Run all examples
    example_concurrent_work()
    example_multiple_async_tasks() 
    example_progress_monitoring()
    example_python_vs_powershell_race()
    
    print("\n🎉 All examples completed successfully!")
