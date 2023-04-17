open System.IO
open System.Collections.Generic
open System.Text.RegularExpressions

let readChunksAsync (filePath: string) =
    async {
        let reader = new StreamReader(filePath)
        let header = reader.ReadLine()
        use fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize = 65536, useAsync = true)
        let mutable totalBytesRead = 0L
        let mutable buffer = Array.zeroCreate<byte> 1024 
        let mutable wordCounts = new System.Collections.Concurrent.ConcurrentBag()
        let rec loop() = async {
            let! bytesRead = fs.ReadAsync(buffer, 0, buffer.Length) |> Async.AwaitTask
            if bytesRead > 0 then
                totalBytesRead <- totalBytesRead + int64 bytesRead
                let content = System.Text.Encoding.Default.GetString(buffer, 0, bytesRead)
                let records = content.Split("\n")
                let mutable res = false

                for record in records do
                    let splitted = record.Split(", ")
                    if splitted.Length = 3 then
                        let id = splitted[0]
                        let name = splitted[1]
                        try 
                            let mutable population = int64 splitted[2]
                            let mutable city = $"{id} {name}"
                            if wordCounts.Count >= 100 then
                                for record in wordCounts do
                                    if record.Value > population then
                                        record.Key = city
                                        record.Value = population
                            else
                                res <- wordCounts.TryAdd(city, population)
                        with
                            | :? System.FormatException ->
                                res <- false
                        
                return! loop()
        }
        let tasks = 
            [1..16]
            |> List.map (fun _ -> loop())
            |> Async.Parallel
        do! Async.Ignore(tasks)
        printfn "Total bytes read: %d" totalBytesRead
        return wordCounts
    }

let writeToFile (path: string, dict: System.Collections.Concurrent.ConcurrentDictionary<string, int64>) = 
    use writer = new StreamWriter(path)
    for word in dict do
        writer.Write($"{word.Key}-{word.Value}\n")

let timer = System.Diagnostics.Stopwatch.StartNew()
let filePath = "./cities.csv"
let result = Async.RunSynchronously (readChunksAsync filePath)
timer.Stop()
printf "result time: %d ms" timer.ElapsedMilliseconds

writeToFile("result.txt", result)