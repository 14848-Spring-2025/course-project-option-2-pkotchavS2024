# MapReduce Test Script for Shakespeare files

# Step 1: Create a temporary directory for our experiment
$tempDir = New-Item -ItemType Directory -Path ".\mapreduce_test" -Force

# Step 2: Use existing Shakespeare files - specify the paths to two of your files
$file1Path = ".\processed_files\shakespeare.tar\histories\1kinghenryiv.txt"
$file2Path = ".\processed_files\shakespeare.tar\histories\kingjohn.txt"

# Check if files exist
if (-not (Test-Path $file1Path)) {
    Write-Error "File not found: $file1Path"
    exit
}
if (-not (Test-Path $file2Path)) {
    Write-Error "File not found: $file2Path"
    exit
}

# Step 3: Set the environment variable for the first file
$env:map_input_file = "histories/1kinghenryiv.txt"

# Step 4: Run the mapper on the first file and save output
Get-Content $file1Path | python mapper.py > "$tempDir\mapper_output1.txt"

# Step 5: Change the environment variable for the second file
$env:map_input_file = "histories/kingjohn.txt"

# Step 6: Run the mapper on the second file and append output
Get-Content $file2Path | python mapper.py > "$tempDir\mapper_output2.txt"

# Step 7: Combine and sort the mapper outputs
Get-Content "$tempDir\mapper_output1.txt", "$tempDir\mapper_output2.txt" | Sort-Object > "$tempDir\sorted_mapper_output.txt"

# Step 8: Run the reducer
Get-Content "$tempDir\sorted_mapper_output.txt" | python reducer.py > "$tempDir\reducer_output.txt"

# Step 9: Display the result (first 20 lines)
Write-Host "Inverted Index Results (first 20 lines):"
Get-Content "$tempDir\reducer_output.txt" -TotalCount 20

Write-Host "Complete! Full results saved to $tempDir\reducer_output.txt"