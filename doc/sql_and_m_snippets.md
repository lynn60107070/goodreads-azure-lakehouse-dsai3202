# SQL and M Script Snippets

## Purpose
Authoritative, human-readable extracts from the Fabric Dataflow export JSON. Power Query M for each shared query, schema summaries, and minimal SQL used downstream.

---

## Power Query M (from JSON)

```m
shared main = let
  Source = AzureStorage.DataLake(
  "https://goodreadsreviews60107070.dfs.core.windows.net/lakehouse/gold/curated_reviews/",
  [HierarchicalNavigation=true]
  ),
  DeltaTable = DeltaLake.Table(Source),
  #"Changed column type" = Table.TransformColumnTypes(DeltaTable, {{"review_id", type text}, {"book_id", type text}, {"title", type text}, {"author_id", type text}, {"name", type text}, {"user_id", type text}, {"rating", Int64.Type}, {"review_text", type text}, {"language_code", type text}, {"n_votes", Int64.Type}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "date_added_iso", each let
    txt    = [date_added],
    parts  = Text.Split(Text.Trim(txt), " "),          // ["Fri","Jul","27","14:06:28","-0700","2012"]
    monAbbr= try parts{1} otherwise null,
    dayTxt = try parts{2} otherwise null,
    yearTxt= try parts{5} otherwise null,

    monNum = try Record.Field(
                [Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12],
                monAbbr) otherwise null,

    iso = if monNum <> null and dayTxt <> null and yearTxt <> null then
            Text.From(Number.FromText(yearTxt)) & "-" &
            Text.PadStart(Text.From(monNum), 2, "0") & "-" &
            Text.PadStart(Text.From(Number.FromText(dayTxt)), 2, "0")
          else null
in
    iso), {{"date_added_iso", type date}}),
  #"Removed blank rows" = Table.SelectRows(#"Added custom", each [rating] <> null),
  #"Removed blank rows 1" = Table.SelectRows(#"Removed blank rows", each [book_id] <> null and [book_id] <> ""),
  #"Removed blank rows 2" = Table.SelectRows(#"Removed blank rows 1",  each [review_text] <> null and [review_text] <> ""),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Removed blank rows 2", "review_length", each Text.Length(Text.Trim([review_text]))), {{"review_length", Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Added custom 1", each [review_length] >= 10),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Filtered rows", {"date_added_iso"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed errors", each [date_added_iso] < #date(2025, 11, 7)),
  #"Replaced value" = Table.ReplaceValue(#"Filtered rows 1", null, 0, Replacer.ReplaceValue, {"n_votes"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "", "Unknown", Replacer.ReplaceValue, {"language_code"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value 1", {{"title", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 1" = Table.TransformColumns(#"Trimmed text", {{"name", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 2" = Table.TransformColumns(#"Trimmed text 1", {{"review_text", each Text.Trim(_), type nullable text}}),
  #"Capitalized each word" = Table.TransformColumns(#"Trimmed text 2", {{"title", each Text.Proper(_), type nullable text}}),
  #"Capitalized each word 1" = Table.TransformColumns(#"Capitalized each word", {{"name", each Text.Proper(_), type nullable text}}),
  #"Merged queries" = Table.NestedJoin(#"Capitalized each word 1", {"book_id"}, avg_rating_bookid, {"book_id"}, "Query (3)", JoinKind.LeftOuter),
  #"From Value" = Table.FromValue(#"Merged queries"),
  #"Remove Columns" = Table.RemoveColumns(#"From Value", Table.ColumnsOfType(#"From Value", {type table, type record, type list, type nullable binary, type binary, type function}))
in
  #"Remove Columns";

shared avg_rating_bookid = let
  Source = AzureStorage.DataLake(
  "https://goodreadsreviews60107070.dfs.core.windows.net/lakehouse/gold/curated_reviews/",
  [HierarchicalNavigation=true]
  ),
  DeltaTable = DeltaLake.Table(Source),
  #"Changed column type" = Table.TransformColumnTypes(DeltaTable, {{"review_id", type text}, {"book_id", type text}, {"title", type text}, {"author_id", type text}, {"name", type text}, {"user_id", type text}, {"rating", Int64.Type}, {"review_text", type text}, {"language_code", type text}, {"n_votes", Int64.Type}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "date_added_iso", each let
    txt    = [date_added],
    parts  = Text.Split(Text.Trim(txt), " "),
    monAbbr= try parts{1} otherwise null,
    dayTxt = try parts{2} otherwise null,
    yearTxt= try parts{5} otherwise null,

    monNum = try Record.Field(
                [Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12],
                monAbbr) otherwise null,

    iso = if monNum <> null and dayTxt <> null and yearTxt <> null then
            Text.From(Number.FromText(yearTxt)) & "-" &
            Text.PadStart(Text.From(monNum), 2, "0") & "-" &
            Text.PadStart(Text.From(Number.FromText(dayTxt)), 2, "0")
          else null
in
    iso), {{"date_added_iso", type date}}),
  #"Removed blank rows" = Table.SelectRows(#"Added custom", each [rating] <> null),
  #"Removed blank rows 1" = Table.SelectRows(#"Removed blank rows", each [book_id] <> null and [book_id] <> ""),
  #"Removed blank rows 2" = Table.SelectRows(#"Removed blank rows 1",  each [review_text] <> null and [review_text] <> ""),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Removed blank rows 2", "review_length", each Text.Length(Text.Trim([review_text]))), {{"review_length", Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Added custom 1", each [review_length] >= 10),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Filtered rows", {"date_added_iso"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed errors", each [date_added_iso] < #date(2025, 11, 7)),
  #"Replaced value" = Table.ReplaceValue(#"Filtered rows 1", null, 0, Replacer.ReplaceValue, {"n_votes"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "", "Unknown", Replacer.ReplaceValue, {"language_code"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value 1", {{"title", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 1" = Table.TransformColumns(#"Trimmed text", {{"name", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 2" = Table.TransformColumns(#"Trimmed text 1", {{"review_text", each Text.Trim(_), type nullable text}}),
  #"Capitalized each word" = Table.TransformColumns(#"Trimmed text 2", {{"title", each Text.Proper(_), type nullable text}}),
  #"Capitalized each word 1" = Table.TransformColumns(#"Capitalized each word", {{"name", each Text.Proper(_), type nullable text}}),
  #"Grouped rows" = Table.Group(#"Capitalized each word 1", {"book_id"}, {{"avg_rating", each List.Average([rating]), type nullable number}})
in
  #"Grouped rows";

shared review_count = let
  Source = AzureStorage.DataLake(
  "https://goodreadsreviews60107070.dfs.core.windows.net/lakehouse/gold/curated_reviews/",
  [HierarchicalNavigation=true]
  ),
  DeltaTable = DeltaLake.Table(Source),
  #"Changed column type" = Table.TransformColumnTypes(DeltaTable, {{"review_id", type text}, {"book_id", type text}, {"title", type text}, {"author_id", type text}, {"name", type text}, {"user_id", type text}, {"rating", Int64.Type}, {"review_text", type text}, {"language_code", type text}, {"n_votes", Int64.Type}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "date_added_iso", each let
    txt    = [date_added],
    parts  = Text.Split(Text.Trim(txt), " "),
    monAbbr= try parts{1} otherwise null,
    dayTxt = try parts{2} otherwise null,
    yearTxt= try parts{5} otherwise null,

    monNum = try Record.Field(
                [Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12],
                monAbbr) otherwise null,

    iso = if monNum <> null and dayTxt <> null and yearTxt <> null then
            Text.From(Number.FromText(yearTxt)) & "-" &
            Text.PadStart(Text.From(monNum), 2, "0") & "-" &
            Text.PadStart(Text.From(Number.FromText(dayTxt)), 2, "0")
          else null
in
    iso), {{"date_added_iso", type date}}),
  #"Removed blank rows" = Table.SelectRows(#"Added custom", each [rating] <> null),
  #"Removed blank rows 1" = Table.SelectRows(#"Removed blank rows", each [book_id] <> null and [book_id] <> ""),
  #"Removed blank rows 2" = Table.SelectRows(#"Removed blank rows 1",  each [review_text] <> null and [review_text] <> ""),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Removed blank rows 2", "review_length", each Text.Length(Text.Trim([review_text]))), {{"review_length", Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Added custom 1", each [review_length] >= 10),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Filtered rows", {"date_added_iso"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed errors", each [date_added_iso] < #date(2025, 11, 7)),
  #"Replaced value" = Table.ReplaceValue(#"Filtered rows 1", null, 0, Replacer.ReplaceValue, {"n_votes"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "", "Unknown", Replacer.ReplaceValue, {"language_code"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value 1", {{"title", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 1" = Table.TransformColumns(#"Trimmed text", {{"name", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 2" = Table.TransformColumns(#"Trimmed text 1", {{"review_text", each Text.Trim(_), type nullable text}}),
  #"Capitalized each word" = Table.TransformColumns(#"Trimmed text 2", {{"title", each Text.Proper(_), type nullable text}}),
  #"Capitalized each word 1" = Table.TransformColumns(#"Capitalized each word", {{"name", each Text.Proper(_), type nullable text}}),
  #"Merged queries" = Table.NestedJoin(#"Capitalized each word 1", {"book_id"}, avg_rating_bookid, {"book_id"}, "Query (3)", JoinKind.LeftOuter),
  #"Grouped rows" = Table.Group(#"Merged queries", {"book_id"}, {{"review_count", each Table.RowCount(_), Int64.Type}})
in
  #"Grouped rows";

shared avg_rating_authorname = let
  Source = AzureStorage.DataLake(
  "https://goodreadsreviews60107070.dfs.core.windows.net/lakehouse/gold/curated_reviews/",
  [HierarchicalNavigation=true]
  ),
  DeltaTable = DeltaLake.Table(Source),
  #"Changed column type" = Table.TransformColumnTypes(DeltaTable, {{"review_id", type text}, {"book_id", type text}, {"title", type text}, {"author_id", type text}, {"name", type text}, {"user_id", type text}, {"rating", Int64.Type}, {"review_text", type text}, {"language_code", type text}, {"n_votes", Int64.Type}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "date_added_iso", each let
    txt    = [date_added],
    parts  = Text.Split(Text.Trim(txt), " "),
    monAbbr= try parts{1} otherwise null,
    dayTxt = try parts{2} otherwise null,
    yearTxt= try parts{5} otherwise null,

    monNum = try Record.Field(
                [Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12],
                monAbbr) otherwise null,

    iso = if monNum <> null and dayTxt <> null and yearTxt <> null then
            Text.From(Number.FromText(yearTxt)) & "-" &
            Text.PadStart(Text.From(monNum), 2, "0") & "-" &
            Text.PadStart(Text.From(Number.FromText(dayTxt)), 2, "0")
          else null
in
    iso), {{"date_added_iso", type date}}),
  #"Removed blank rows" = Table.SelectRows(#"Added custom", each [rating] <> null),
  #"Removed blank rows 1" = Table.SelectRows(#"Removed blank rows", each [book_id] <> null and [book_id] <> ""),
  #"Removed blank rows 2" = Table.SelectRows(#"Removed blank rows 1",  each [review_text] <> null and [review_text] <> ""),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Removed blank rows 2", "review_length", each Text.Length(Text.Trim([review_text]))), {{"review_length", Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Added custom 1", each [review_length] >= 10),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Filtered rows", {"date_added_iso"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed errors", each [date_added_iso] < #date(2025, 11, 7)),
  #"Replaced value" = Table.ReplaceValue(#"Filtered rows 1", null, 0, Replacer.ReplaceValue, {"n_votes"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "", "Unknown", Replacer.ReplaceValue, {"language_code"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value 1", {{"title", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 1" = Table.TransformColumns(#"Trimmed text", {{"name", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 2" = Table.TransformColumns(#"Trimmed text 1", {{"review_text", each Text.Trim(_), type nullable text}}),
  #"Capitalized each word" = Table.TransformColumns(#"Trimmed text 2", {{"title", each Text.Proper(_), type nullable text}}),
  #"Capitalized each word 1" = Table.TransformColumns(#"Capitalized each word", {{"name", each Text.Proper(_), type nullable text}}),
  #"Merged queries" = Table.NestedJoin(#"Capitalized each word 1", {"book_id"}, avg_rating_bookid, {"book_id"}, "Query (3)", JoinKind.LeftOuter),
  #"Grouped rows" = Table.Group(#"Merged queries", {"author_id"}, {{"avg_author_rating", each List.Average([rating]), type nullable number}})
in
  #"Grouped rows";

shared word_count_reviews = let
  Source = AzureStorage.DataLake(
  "https://goodreadsreviews60107070.dfs.core.windows.net/lakehouse/gold/curated_reviews/",
  [HierarchicalNavigation=true]
  ),
  DeltaTable = DeltaLake.Table(Source),
  #"Changed column type" = Table.TransformColumnTypes(DeltaTable, {{"review_id", type text}, {"book_id", type text}, {"title", type text}, {"author_id", type text}, {"name", type text}, {"user_id", type text}, {"rating", Int64.Type}, {"review_text", type text}, {"language_code", type text}, {"n_votes", Int64.Type}}),
  #"Added custom" = Table.TransformColumnTypes(Table.AddColumn(#"Changed column type", "date_added_iso", each let
    txt    = [date_added],
    parts  = Text.Split(Text.Trim(txt), " "),
    monAbbr= try parts{1} otherwise null,
    dayTxt = try parts{2} otherwise null,
    yearTxt= try parts{5} otherwise null,

    monNum = try Record.Field(
                [Jan=1,Feb=2,Mar=3,Apr=4,May=5,Jun=6,Jul=7,Aug=8,Sep=9,Oct=10,Nov=11,Dec=12],
                monAbbr) otherwise null,

    iso = if monNum <> null and dayTxt <> null and yearTxt <> null then
            Text.From(Number.FromText(yearTxt)) & "-" &
            Text.PadStart(Text.From(monNum), 2, "0") & "-" &
            Text.PadStart(Text.From(Number.FromText(dayTxt)), 2, "0")
          else null
in
    iso), {{"date_added_iso", type date}}),
  #"Removed blank rows" = Table.SelectRows(#"Added custom", each [rating] <> null),
  #"Removed blank rows 1" = Table.SelectRows(#"Removed blank rows", each [book_id] <> null and [book_id] <> ""),
  #"Removed blank rows 2" = Table.SelectRows(#"Removed blank rows 1",  each [review_text] <> null and [review_text] <> ""),
  #"Added custom 1" = Table.TransformColumnTypes(Table.AddColumn(#"Removed blank rows 2", "review_length", each Text.Length(Text.Trim([review_text]))), {{"review_length", Int64.Type}}),
  #"Filtered rows" = Table.SelectRows(#"Added custom 1", each [review_length] >= 10),
  #"Removed errors" = Table.RemoveRowsWithErrors(#"Filtered rows", {"date_added_iso"}),
  #"Filtered rows 1" = Table.SelectRows(#"Removed errors", each [date_added_iso] < #date(2025, 11, 7)),
  #"Replaced value" = Table.ReplaceValue(#"Filtered rows 1", null, 0, Replacer.ReplaceValue, {"n_votes"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "", "Unknown", Replacer.ReplaceValue, {"language_code"}),
  #"Trimmed text" = Table.TransformColumns(#"Replaced value 1", {{"title", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 1" = Table.TransformColumns(#"Trimmed text", {{"name", each Text.Trim(_), type nullable text}}),
  #"Trimmed text 2" = Table.TransformColumns(#"Trimmed text 1", {{"review_text", each Text.Trim(_), type nullable text}}),
  #"Capitalized each word" = Table.TransformColumns(#"Trimmed text 2", {{"title", each Text.Proper(_), type nullable text}}),
  #"Capitalized each word 1" = Table.TransformColumns(#"Capitalized each word", {{"name", each Text.Proper(_), type nullable text}}),
  #"Merged queries" = Table.NestedJoin(#"Capitalized each word 1", {"book_id"}, avg_rating_bookid, {"book_id"}, "Query (3)", JoinKind.LeftOuter),
  #"From Value" = Table.FromValue(#"Merged queries"),
  #"Remove Columns" = Table.RemoveColumns(#"From Value", Table.ColumnsOfType(#"From Value", {type table, type record, type list, type nullable binary, type binary, type function}))
in
  #"Remove Columns";

