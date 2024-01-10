using LinqToDB;
using LinqToDB.Data;

var db = new DataConnection(new DataOptions().UsePostgreSQL("Server=localhost;Port=5432;Database=test;User Id=postgres; Password=UzTWKjB21WHMyMg3MEZe;"));
var tbl = db.GetTable<dto>();

void PrintResults(List<dto> d)
{
    foreach (var a in d)
        Console.WriteLine($"{a.id}, {a.name}, {a.FullName}, {a.parent_id}");
}

void PrintResultsMapped(List<DtoMapped> d)
{
    foreach (var a in d)
        Console.WriteLine($"{a.Dto.id}, {a.Dto.name}, {a.FullName}, {a.Dto.parent_id}");
}

// works
var results = await db.GetCte<dto>(d =>
    (from a in tbl
        where a.parent_id == null
        select new dto
        {
            id = a.id,
            parent_id = a.parent_id,
            name = a.name,
            FullName = a.name
        })
    .Concat(
        from b in tbl
        from recur in d.InnerJoin(dd => dd.id == b.parent_id)
        select new dto
        {
            id = b.id,
            parent_id = b.parent_id,
            name = b.name,
            FullName = recur.FullName + " > " + b.name
        }
    )
)
    .ToListAsync();

PrintResults(results);

// does not work
var resultsMapped = await db.GetCte<DtoMapped>(d =>
    (from a in tbl
        where a.parent_id == null
        select new DtoMapped
        {
            Dto = a,
            FullName = a.name
        })
    .Concat(
        from b in tbl
        from recur in d.InnerJoin(dd => dd.Dto.id == b.parent_id)
        select new DtoMapped
        {
            Dto = b,
            FullName = recur.FullName + " > " + b.name
        }
    )
)
    .ToListAsync();

PrintResultsMapped(resultsMapped);

class dto
{
    public int id { get; set; }
    public string name { get; set; }
    public int? parent_id { get; set; }
    public string? FullName;
}

class DtoMapped
{
    public dto Dto { get; set; }
    public string? FullName;
}