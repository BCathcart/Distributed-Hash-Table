for($i = 0; $i -lt 10; $i++){
    start powershell "go run --race src/server/pa2server.go 4422$($i) peers.txt > 4422$($i).log 2>&1"
    # Start-Process -FilePath "go run --race src/server/pa2server.go" -ArgumentList "4422$i", "peers.txt" -RedirectStandardOutput "Sorted.txt"
}
for($i = 0; $i -lt 10; $i++){
    start powershell "go run --race src/server/pa2server.go 4423$($i) peers.txt > 4423$($i).log 2>&1"
}
for($i = 0; $i -lt 10; $i++){
    start powershell "go run --race src/server/pa2server.go 4424$($i) peers.txt > 4424$($i).log 2>&1"
}
for($i = 0; $i -lt 5; $i++){
    start powershell "go run --race src/server/pa2server.go 4425$($i) peers.txt > 4425$($i).log 2>&1"
}