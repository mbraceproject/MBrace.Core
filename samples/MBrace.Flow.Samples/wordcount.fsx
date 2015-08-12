#I "../../bin/"
#r "MBrace.Core"
#r "MBrace.Runtime.Core"
#r "Streams.Core"
#r "MBrace.Flow"
#r "MBrace.Thespian"

open System
open System.IO
open System.Text.RegularExpressions
open Nessos.Streams
open MBrace.Core
open MBrace.Store
open MBrace.Thespian
open MBrace.Flow

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

// Splits a string into words
let splitWords =
    let regex = new Regex(@"[\W]+", RegexOptions.Compiled)
    fun text -> regex.Split(text)

let wordTransform (word : string) = word.Trim().ToLower()

let wordFilter (word : string) = word.Length > 3 && not <| noiseWords.Contains(word)

let files = Directory.GetFiles @"path to files"

MBraceThespian.WorkerExecutable <- Path.Combine(__SOURCE_DIRECTORY__, "../../bin/MBrace.Thespian.exe")
let runtime = MBraceThespian.InitLocal(4)
let storeClient = runtime.StoreClient

//
// CloudVector API
//

let words =
    CloudFlow.OfCloudFilesByLine files
    |> CloudFlow.collect (fun line -> splitWords line |> Seq.map wordTransform)
    |> CloudFlow.persist
    |> runtime.Run

let getTop count =
    words
    |> CloudFlow.collect (fun line -> splitWords line |> Seq.map wordTransform)
    |> CloudFlow.filter wordFilter
    |> CloudFlow.countBy id
    |> CloudFlow.sortBy (fun (_,c) -> -c) count
    |> CloudFlow.persist

             
let persistedFlow = runtime.Run(getTop 20)

persistedFlow.ToEnumerable()
|> runtime.RunOnCurrentProcess
|> Seq.iter (printfn "%A")