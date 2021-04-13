<#
    Utility script for windows users, runs server at node 8080 + first argument,
    # logging the output in a serverXLog.txt file. Currently works with up to 10
    nodes.
 #>
 #>
#>
$param1=$args[0];
$param1Int = [int]$param1;
$port = $param1Int + 8080; #Edit this to your base port.
$textFile = -join("logs\server", $param1, "Log.txt");
if ($param1Int -eq 0) { #The first node to join will have an empty peers file.
    go run .\src\server\pa2server.go $port initPeers.txt 2> $textFile;
}
else {
    go run .\src\server\pa2server.go $port peers.txt 2> $textFile;
}