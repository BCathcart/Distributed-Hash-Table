:: Deletes All files in the Current Directory With Prompts and Warnings 
::(Hidden, System, and Read-Only Files are Not Affected) 
:: 
@ECHO OFF
for /l %%x in (0, 1, 9) do (
START go run src/server/pa2server.go 4422%%x peers.txt 
)
for /l %%x in (0, 1, 9) do (
START go run src/server/pa2server.go 4423%%x peers.txt 
)
for /l %%x in (0, 1, 9) do (
START go run src/server/pa2server.go 4424%%x peers.txt 
)
for /l %%x in (0, 1, 9) do (
START go run src/server/pa2server.go 4425%%x peers.txt 
)
@REM for /l %%x in (0, 1, 9) do (
@REM START go run src/server/pa2server.go 4426%%x peers.txt 
@REM )
PAUSE