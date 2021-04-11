$param1=$args[0];
$param1Int = [int]$param1;
$port = $param1Int + 8080;
$textFile = -join("logs\server", $param1, "Log.txt");
if ($param1Int -eq 0) {
    go run .\src\server\pa2server.go $port initPeers.txt 2> $textFile;
}
else {
    go run .\src\server\pa2server.go $port peers.txt 2> $textFile;
}