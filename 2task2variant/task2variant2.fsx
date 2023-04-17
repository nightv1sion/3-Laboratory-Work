open System.IO
open System.Collections.Generic
open System.Text.RegularExpressions

let readChunksAsync (filePath: string) =
    async {
        use fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, bufferSize = 4096, useAsync = true)
        let mutable totalBytesRead = 0L
        let mutable buffer = Array.zeroCreate<byte> 65536 // 64 KB buffer
        let mutable wordCounts = System.Collections.Concurrent.ConcurrentDictionary<string, int>()
        let rec loop() = async {
            let! bytesRead = fs.ReadAsync(buffer, 0, buffer.Length) |> Async.AwaitTask
            if bytesRead > 0 then
                totalBytesRead <- totalBytesRead + int64 bytesRead
                let content = System.Text.Encoding.Default.GetString(buffer, 0, bytesRead)
                let noPunctuation = content.Replace(".", "").Replace(",", "").Replace("-", "").ToLower();
                let words = noPunctuation.Split [|' '; '\t'; '\n'; '\r'|] |> Array.filter (fun s -> s <> "")
                for word in words do
                    if wordCounts.ContainsKey word then
                        let count = wordCounts.[word] + 1
                        wordCounts.[word] <- count
                    else
                        wordCounts.TryAdd(word, 1)
                return! loop()
        }
        let tasks = 
            [1..10]
            |> List.map (fun _ -> loop())
            |> Async.Parallel
        do! Async.Ignore(tasks)
        printfn "Total bytes read: %d" totalBytesRead
        return wordCounts
    }

let writeToFile (path: string, dict: System.Collections.Concurrent.ConcurrentDictionary<string, int>) = 
    use writer = new StreamWriter(path)
    for word in dict do
        writer.Write($"{word.Key}-{word.Value}\n")

let timer = System.Diagnostics.Stopwatch.StartNew()
let filePath = "./document.txt"
let result = Async.RunSynchronously (readChunksAsync filePath)
timer.Stop()
printf "result time: %d ms" timer.ElapsedMilliseconds

writeToFile("result.txt", result)