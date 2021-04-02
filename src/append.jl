"""
    Arrow.append(file::String, tbl)
    tbl |> Arrow.append(io_or_file)

Append any [Tables.jl](https://github.com/JuliaData/Tables.jl)-compatible
`tbl` to an existing arrow formatted file.

Multiple record batches will be written based on the number of
`Tables.partitions(tbl)` that are provided; by default, this is just
one for a given table, but some table sources support automatic
partitioning. Note you can turn multiple table objects into partitions
by doing `Tables.partitioner([tbl1, tbl2, ...])`, but note that
each table must have the exact same `Tables.Schema`.

By default, `Arrow.append` will use multiple threads to write multiple
record batches simultaneously (e.g. if julia is started with `julia -t 8`
or the `JULIA_NUM_THREADS` environment variable is set).

Supported keyword arguments to `Arrow.append` include:
  * `compress`: possible values include `:lz4`, `:zstd`, or your own initialized `LZ4FrameCompressor` or `ZstdCompressor` objects; will cause all buffers in each record batch to use the respective compression encoding
  * `alignment::Int=8`: specify the number of bytes to align buffers to when written in messages; strongly recommended to only use alignment values of 8 or 64 for modern memory cache line optimization
  * `dictencode::Bool=false`: whether all columns should use dictionary encoding when being written; to dict encode specific columns, wrap the column/array in `Arrow.DictEncode(col)`
  * `dictencodenested::Bool=false`: whether nested data type columns should also dict encode nested arrays/buffers; other language implementations [may not support this](https://arrow.apache.org/docs/status.html)
  * `denseunions::Bool=true`: whether Julia `Vector{<:Union}` arrays should be written using the dense union layout; passing `false` will result in the sparse union layout
  * `largelists::Bool=false`: causes list column types to be written with Int64 offset arrays; mainly for testing purposes; by default, Int64 offsets will be used only if needed
  * `maxdepth::Int=$DEFAULT_MAX_DEPTH`: deepest allowed nested serialization level; this is provided by default to prevent accidental infinite recursion with mutually recursive data structures
  * `ntasks::Int`: number of concurrent threaded tasks to allow while writing input partitions out as arrow record batches; default is no limit; to disable multithreaded writing, pass `ntasks=1`
"""
function append end

append(file::String; kw...) = x -> append(file::String, x; kw...)

function append(file::String, tbl;
        largelists::Bool=false,
        compress::Union{Nothing, Symbol, LZ4FrameCompressor, ZstdCompressor}=nothing,
        denseunions::Bool=true,
        dictencode::Bool=false,
        dictencodenested::Bool=false,
        alignment::Int=8,
        maxdepth::Int=DEFAULT_MAX_DEPTH,
        ntasks=Inf)
    if ntasks < 1
        throw(ArgumentError("ntasks keyword argument must be > 0; pass `ntasks=1` to disable multithreaded writing"))
    end
    if compress === :lz4
        compress = LZ4_FRAME_COMPRESSOR
    elseif compress === :zstd
        compress = ZSTD_COMPRESSOR
    elseif compress isa Symbol
        throw(ArgumentError("unsupported compress keyword argument value: $compress. Valid values include `:lz4` or `:zstd`"))
    end

    open(file, "r+") do io
        append(io, tbl, largelists, compress, denseunions, dictencode, dictencodenested, alignment, maxdepth, ntasks)
    end

    return file
end

function append(io, source, largelists, compress, denseunions, dictencode, dictencodenested, alignment, maxdepth, ntasks)
    bytes = Mmap.mmap(io)
    stream = Arrow.Stream(bytes)
    arrow_schema = Tables.schema(Tables.columns(first(stream)))
    dictencodings = stream.dictencodings
    firstcols = toarrowtable(stream, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested, maxdepth)
    sch = Ref{Tables.Schema}(Tables.schema(firstcols))

    # position file at the last footer
    offset = footer_offset(bytes)
    seek(io, length(bytes) - offset)

    # TODO: we're probably not threadsafe if user passes own single compressor instance + ntasks > 1
    # if ntasks > 1 && compres !== nothing && !(compress isa Vector)
    #     compress = Threads.resize_nthreads!([compress])
    # end
    msgs = OrderedChannel{Message}(ntasks)
    # build messages
    blocks = (Block[], Block[])
    # start message writing from channel
    threaded = ntasks > 1
    tsk = threaded ? (Threads.@spawn for msg in msgs
        Base.write(io, msg, blocks, sch, alignment)
    end) : (@async for msg in msgs
        Base.write(io, msg, blocks, sch, alignment)
    end)
    anyerror = Threads.Atomic{Bool}(false)
    errorref = Ref{Any}()
    @sync for (i, tbl) in enumerate(Tables.partitions(source))
        if anyerror[]
            @error "error writing arrow data on partition = $(errorref[][3])" exception=(errorref[][1], errorref[][2])
            error("fatal error writing arrow data")
        end
        @debug 1 "processing table partition i = $i"
        tbl_schema = Tables.schema(tbl)

        if !is_equivalent_schema(arrow_schema, tbl_schema)
            throw(ArgumentError("Table schema does not match existing arrow file schema"))
        end

        if threaded
            Threads.@spawn process_partition(tbl, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested, maxdepth, msgs, alignment, i, sch, errorref, anyerror)
        else
            @async process_partition(tbl, dictencodings, largelists, compress, denseunions, dictencode, dictencodenested, maxdepth, msgs, alignment, i, sch, errorref, anyerror)
        end
    end
    if anyerror[]
        @error "error writing arrow data on partition = $(errorref[][3])" exception=(errorref[][1], errorref[][2])
        error("fatal error writing arrow data")
    end
    # close our message-writing channel, no further put!-ing is allowed
    close(msgs)
    # now wait for our message-writing task to finish writing
    wait(tsk)

    b = FlatBuffers.Builder(1024)
    schfoot = makeschema(b, sch[], firstcols)
    if !isempty(blocks[1])
        N = length(blocks[1])
        Meta.footerStartRecordBatchesVector(b, N)
        for blk in Iterators.reverse(blocks[1])
            Meta.createBlock(b, blk.offset, blk.metaDataLength, blk.bodyLength)
        end
        recordbatches = FlatBuffers.endvector!(b, N)
    else
        recordbatches = FlatBuffers.UOffsetT(0)
    end
    if !isempty(blocks[2])
        N = length(blocks[2])
        Meta.footerStartDictionariesVector(b, N)
        for blk in Iterators.reverse(blocks[2])
            Meta.createBlock(b, blk.offset, blk.metaDataLength, blk.bodyLength)
        end
        dicts = FlatBuffers.endvector!(b, N)
    else
        dicts = FlatBuffers.UOffsetT(0)
    end
    Meta.footerStart(b)
    Meta.footerAddVersion(b, Meta.MetadataVersion.V4)
    Meta.footerAddSchema(b, schfoot)
    Meta.footerAddDictionaries(b, dicts)
    Meta.footerAddRecordBatches(b, recordbatches)
    foot = Meta.footerEnd(b)
    FlatBuffers.finish!(b, foot)
    footer = FlatBuffers.finishedbytes(b)
    Base.write(io, footer)
    Base.write(io, Int32(length(footer)))
    Base.write(io, "ARROW1")

    return io
end

function footer_offset(bytes::Vector{UInt8})
    magic_len = length(FILE_FORMAT_MAGIC_BYTES)
    startoffset = length(bytes) - magic_len - sizeof(Int32) + 1
    endoffset = length(bytes) - magic_len

    read(IOBuffer(bytes[startoffset:endoffset]), Int32) + magic_len + sizeof(Int32)
end

function is_equivalent_schema(sch1::Tables.Schema, sch2::Tables.Schema)
    (sch1.names == sch2.names) || (return false)
    for (t1,t2) in zip(sch1.types, sch2.types)
        (t1 === t2) || (return false)
    end
    true
end