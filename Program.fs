#if INTERACTIVE
#r "System.Data.dll"
#r "FSharp.Data.TypeProviders.dll"
#r "System.Data.Linq.dll"
#endif

open System
open System.Net
open System.Data
open System.Data.Linq
open System.IO
open Microsoft.FSharp.Data.TypeProviders
open Microsoft.FSharp.Linq

let url = "http://ichart.finance.yahoo.com/table.csv?s="

//type dbSchema = SqlDataConnection<"Data Source=thebeast;Initial Catalog=FinancialData;Integrated Security=True">
type dbSchema = SqlDataConnection<"Data Source=GBD03821801\SQLEXPRESS;Initial Catalog=FinancialData;Integrated Security=True">
let db = dbSchema.GetDataContext()

let getStockPrices1 stock =
    let wc = new WebClient()
    let data = wc.DownloadString(url + stock)
    let dataLines = data.Split([| '\n' |], StringSplitOptions.RemoveEmptyEntries) 

    seq { for line in dataLines |> Seq.skip 1 do
              let infos = line.Split(',')
              yield new dbSchema.ServiceTypes.Stocks(Symbol = stock, TradeDate = DateTime.Parse infos.[0], Open = Nullable(float infos.[1]),
                                                           High = Nullable(float infos.[2]), Low = Nullable(float infos.[3]), Close = Nullable(float infos.[4]),
                                                           Volume = Nullable(float infos.[5]), AdjClose = Nullable(float infos.[6]))
              }
    |> Array.ofSeq |> Array.rev

let makeUrl symbol (dfrom:DateTime) (dto:DateTime) = 
    //Uses the not-so-known chart-data:
    let monthfix (d:DateTime)= (d.Month-1).ToString()
    new Uri("http://ichart.finance.yahoo.com/table.csv?s=" + symbol +
        "&e=" + dto.Day.ToString() + "&d=" + monthfix(dto) + "&f=" + dto.Year.ToString() +
        "&g=d&b=" + dfrom.Day.ToString() + "&a=" + monthfix(dfrom) + "&c=" + dfrom.Year.ToString() +
        "&ignore=.csv")


let fetch (url : Uri) = 
    let req = WebRequest.Create (url) :?> HttpWebRequest
    use stream = req.GetResponse().GetResponseStream()
    use reader = new StreamReader(stream)
    reader.ReadToEnd()

let reformat (response:string) = 
    let split (mark:char) (data:string) = 
        data.Split(mark) |> Array.toList
    response |> split '\n' 
    |> List.filter (fun f -> f<>"") 
    |> List.map (split ',') 
    
let getRequest uri = (fetch >> reformat) uri

let getStockPrices symbol fromDate =
    let strQuotes = makeUrl symbol fromDate DateTime.Today  |> getRequest
    seq { for infos in strQuotes |> Seq.skip 1 do
              yield new dbSchema.ServiceTypes.Stocks(Symbol = symbol, TradeDate = DateTime.Parse infos.[0], Open = Nullable(float infos.[1]),
                                                           High = Nullable(float infos.[2]), Low = Nullable(float infos.[3]), Close = Nullable(float infos.[4]),
                                                           Volume = Nullable(float infos.[5]), AdjClose = Nullable(float infos.[6]))
              }
    |> Array.ofSeq |> Array.rev
            

let findMaxDate symbol =
    let row = query {
        for row in db.Stocks do
        where (row.Symbol = symbol)
        sortByDescending row.TradeDate
        headOrDefault
        }
    if row = null then new DateTime(1927, 1, 1) else row.TradeDate.AddDays(+1.)

let updateFedRate () =
    let maxDate = findMaxDate "FedRate"
    let lines =
        (fetch >> reformat) (Uri(@"http://www.federalreserve.gov/datadownload/Output.aspx?rel=H15&series=c5025f4bbbed155a6f17c587772ed69e&lastObs=&from=&to=&filetype=csv&label=include&layout=seriescolumn"))
        |> Seq.skip 6
        |> Seq.map (fun a -> DateTime.Parse(a.[0]), Nullable(float a.[1]))
        |> Seq.filter (fun (d, _) -> d > maxDate)
    lines
    |> Seq.map (fun (d,r) -> new dbSchema.ServiceTypes.Stocks(Symbol = "FedRate", TradeDate = d, Close = r))
    |> db.Stocks.InsertAllOnSubmit
    db.DataContext.SubmitChanges ()

let updateStockPrices symbol = getStockPrices symbol (findMaxDate symbol)

let nfloat (x:string) = Nullable(float x)

let readHistoricalOptionsData file =
    file
    |> File.ReadAllText
    |> reformat
    |> Seq.skip 1
    |> Seq.map (fun o ->
                    new dbSchema.ServiceTypes.Options(
                        Symbol= o.[3], TradeDate = DateTime.ParseExact(o.[7], "MM/dd/yyyy", null), Underlying = o.[0],
                        UndPrice = nfloat o.[1], Expiry = DateTime.ParseExact(o.[6], "MM/dd/yyyy", null), Strike = float o.[8],
                        CallOrPut = (if o.[5] = "call" then 0 else 1), Category = "", Last = nfloat o.[9],
                        Bid = nfloat o.[10], Ask = nfloat o.[11], OpenInt = nfloat o.[13], Volume = nfloat o.[12],
                        ImplVol = nfloat o.[14], Delta = nfloat o.[15], Gamma = nfloat o.[16], Theta = nfloat o.[17],
                        Vega = nfloat o.[18] 
                        ))

let saveOptions options =
    options
    |> db.Options.InsertAllOnSubmit
    db.DataContext.SubmitChanges()

[<EntryPoint>]
let main argv = 
    let stocks = argv
    stocks |> Array.map updateStockPrices |> Array.iter db.Stocks.InsertAllOnSubmit
    db.DataContext.SubmitChanges()
    0 // return an integer exit code
