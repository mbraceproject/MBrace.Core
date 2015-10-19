(* FSI Initialization Code *)
#I @"..\bin\"

#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "MBrace.Thespian.dll"
#r "MBrace.Flow.dll"
#r "Streams.dll"

open MBrace.Core
open MBrace.Library
open MBrace.Thespian
open MBrace.Flow

ThespianWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../bin/mbrace.thespian.worker.exe"

(* WordCount Example using CloudFlow *)

open System
open System.IO
open System.Text.RegularExpressions

/// words ignored by wordcount
let noiseWords = 
    set [
        "a"; "about"; "above"; "all"; "along"; "also"; "although"; "am"; "an"; "any"; "are"; "aren't"; "as"; "at";
        "be"; "because"; "been"; "but"; "by"; "can"; "cannot"; "could"; "couldn't"; "did"; "didn't"; "do"; "does"; 
        "doesn't"; "e.g."; "either"; "etc"; "etc."; "even"; "ever";"for"; "from"; "further"; "get"; "gets"; "got"; 
        "had"; "hardly"; "has"; "hasn't"; "having"; "he"; "hence"; "her"; "here"; "hereby"; "herein"; "hereof"; 
        "hereon"; "hereto"; "herewith"; "him"; "his"; "how"; "however"; "I"; "i.e."; "if"; "into"; "it"; "it's"; "its";
        "me"; "more"; "most"; "mr"; "my"; "near"; "nor"; "now"; "of"; "onto"; "other"; "our"; "out"; "over"; "really"; 
        "said"; "same"; "she"; "should"; "shouldn't"; "since"; "so"; "some"; "such"; "than"; "that"; "the"; "their"; 
        "them"; "then"; "there"; "thereby"; "therefore"; "therefrom"; "therein"; "thereof"; "thereon"; "thereto"; 
        "therewith"; "these"; "they"; "this"; "those"; "through"; "thus"; "to"; "too"; "under"; "until"; "unto"; "upon";
        "us"; "very"; "viz"; "was"; "wasn't"; "we"; "were"; "what"; "when"; "where"; "whereby"; "wherein"; "whether";
        "which"; "while"; "who"; "whom"; "whose"; "why"; "with"; "without"; "would"; "you"; "your" ; "have"; "thou"; "will"; 
        "shall"
    ]

/// Splits a string into words
let splitWords =
    let regex = new Regex(@"[\W]+", RegexOptions.Compiled)
    fun text -> regex.Split(text)

/// Normalizes a word token
let normalize (word : string) = word.Trim().ToLower()

/// Checks if provided word qualifies as noise
let isNoiseWord (word : string) = word.Length < 3 || noiseWords.Contains(word)

/// Computes and caches words across the MBrace cluster
let getWords (urls : seq<string>) =
    CloudFlow.OfHttpFileByLine urls
    |> CloudFlow.collect (fun line -> splitWords line)
    |> CloudFlow.map normalize
    |> CloudFlow.cache

/// Computes the word count using the input cloud flow
let getWordCount (count : int) (words : CloudFlow<string>) =
    words
    |> CloudFlow.collect (fun line -> splitWords line |> Seq.map normalize)
    |> CloudFlow.filter isNoiseWord
    |> CloudFlow.countBy id
    |> CloudFlow.sortBy (fun (_,c) -> -c) count
    |> CloudFlow.toArray

/// initialize a local cluster of 4 workers
let cluster = ThespianCluster.InitOnCurrentMachine(workerCount = 4, logger = new ConsoleLogger())

/// input data set
let testUrls = 
    [| 
        "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-alls-11.txt";
        "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-antony-23.txt";
        "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-as-12.txt";
        "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-comedy-7.txt";
        "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-coriolanus-24.txt";
        "http://ocw.mit.edu/ans7870/6/6.006/s08/lecturenotes/files/t8.shakespeare.txt" 
    |]
             
let words = getWords testUrls |> cluster.Run

let wordCount = getWordCount 10 words |> cluster.Run
