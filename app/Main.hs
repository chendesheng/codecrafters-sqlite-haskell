{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad (forM, forM_, when)
import Data.Binary (Binary (get), Word16, Word32, Word64, Word8)
import Data.Binary.Get qualified as BG
import Data.Bits (Bits (shiftL, (.&.), (.|.)))
import Data.ByteString qualified as BS
import Data.ByteString.Internal qualified as BS (fromForeignPtr)
import Data.ByteString.Lazy qualified as BL
import Data.Function ((&))
import Data.Graph (Table)
import Data.Int (Int64)
import Data.List (elemIndex, find, findIndex, findIndices, intercalate)
import Data.Maybe (fromJust, listToMaybe, mapMaybe)
import Data.Text.Lazy (Text)
import Data.Text.Lazy qualified as T
import Data.Text.Lazy.Encoding (decodeLatin1)
import Data.Text.Lazy.IO qualified as IO
import Debug.Trace (trace, traceShow)
import Foreign (Ptr)
import Foreign.ForeignPtr (newForeignPtr_)
import Foreign.Ptr (castPtr)
import Language.SQL.SimpleSQL.Dialect (Dialect (diAutoincrement), ansi2011)
import Language.SQL.SimpleSQL.Parse qualified as Sql
import Language.SQL.SimpleSQL.Syntax (QueryExpr (..))
import Language.SQL.SimpleSQL.Syntax qualified as Sql
import Numeric (showHex)
import System.Environment (getArgs)
import System.IO.MMap (mmapFileByteString, mmapFileByteStringLazy, mmapFileForeignPtr, mmapWithFilePtr, munmapFilePtr)
import System.IO.MMap qualified as MMap

traceS :: (Show a) => String -> a -> a
traceS prefix x = trace (prefix <> ": " <> show x) x

data DatabaseHeader = DatabaseHeader
    { _dbPageSize :: Word16
    , _dbFileFormatWriteVersion :: Word8
    , _dbFileFormatReadVersion :: Word8
    , _dbReservedBytes :: Word8
    , _dbMaxEmbeddedPayloadFraction :: Word8
    , _dbMinEmbeddedPayloadFraction :: Word8
    , _dbLeafPayloadFraction :: Word8
    , _dbFileChangeCounter :: Word32
    , _dbDatabaseSize :: Word32
    , _dbFirstFreelistTrunkPage :: Word32
    , _dbTotalFreelistPages :: Word32
    , _dbSchemaCookie :: Word32
    , _dbSchemaFormatNumber :: Word32
    , _dbDefaultPageCacheSize :: Word32
    , _dbLargestRootBTreePageNumber :: Word32
    , _dbTextEncoding :: Word32
    , _dbUserVersion :: Word32
    , _dbIncrementalVacuumMode :: Word32
    , _dbApplicationId :: Word32
    , _dbVersionValidFor :: Word32
    , _dbSqliteVersionNumber :: Word32
    }
    deriving (Show)

databaseHeaderParser :: BG.Get DatabaseHeader
databaseHeaderParser = do
    BG.skip 16
    pageSize <- BG.getWord16be
    fileFormatWriteVersion <- BG.getWord8
    fileFormatReadVersion <- BG.getWord8
    reservedBytes <- BG.getWord8
    maxEmbeddedPayloadFraction <- BG.getWord8
    minEmbeddedPayloadFraction <- BG.getWord8
    leafPayloadFraction <- BG.getWord8
    fileChangeCounter <- BG.getWord32be
    databaseSize <- BG.getWord32be
    firstFreelistTrunkPage <- BG.getWord32be
    totalFreelistPages <- BG.getWord32be
    schemaCookie <- BG.getWord32be
    schemaFormatNumber <- BG.getWord32be
    defaultPageCacheSize <- BG.getWord32be
    largestRootBTreePageNumber <- BG.getWord32be
    textEncoding <- BG.getWord32be
    userVersion <- BG.getWord32be
    incrementalVacuumMode <- BG.getWord32be
    applicationId <- BG.getWord32be
    BG.skip 20
    versionValidFor <- BG.getWord32be
    sqliteVersionNumber <- BG.getWord32be
    pure $
        DatabaseHeader
            { _dbPageSize = pageSize -- 1 is for 64KB
            , _dbFileFormatWriteVersion = fileFormatWriteVersion
            , _dbFileFormatReadVersion = fileFormatReadVersion
            , _dbReservedBytes = reservedBytes
            , _dbMaxEmbeddedPayloadFraction = maxEmbeddedPayloadFraction
            , _dbMinEmbeddedPayloadFraction = minEmbeddedPayloadFraction
            , _dbLeafPayloadFraction = leafPayloadFraction
            , _dbFileChangeCounter = fileChangeCounter
            , _dbDatabaseSize = databaseSize
            , _dbFirstFreelistTrunkPage = firstFreelistTrunkPage
            , _dbTotalFreelistPages = totalFreelistPages
            , _dbSchemaCookie = schemaCookie
            , _dbSchemaFormatNumber = schemaFormatNumber
            , _dbDefaultPageCacheSize = defaultPageCacheSize
            , _dbLargestRootBTreePageNumber = largestRootBTreePageNumber
            , _dbTextEncoding = textEncoding
            , _dbUserVersion = userVersion
            , _dbIncrementalVacuumMode = incrementalVacuumMode
            , _dbApplicationId = applicationId
            , _dbVersionValidFor = versionValidFor
            , _dbSqliteVersionNumber = sqliteVersionNumber
            }

data PageType = IndexInteriorPage | IndexLeafPage | TableInteriorPage | TableLeafPage
    deriving (Show, Eq)

data PageHeader = PageHeader
    { _pageType :: PageType
    , _firstFreeblockOffset :: Word16
    , _numberOfCells :: Word16
    , _cellContentAreaStartOffset :: Word16
    , _fragmentedFreeBytes :: Word8
    , _rightmostPointer :: Maybe Word32
    }
    deriving (Show)

pageTypeParser :: BG.Get PageType
pageTypeParser = do
    pageType <- BG.getWord8
    case pageType of
        0x02 -> pure IndexInteriorPage
        0x05 -> pure TableInteriorPage
        0x0A -> pure IndexLeafPage
        0x0D -> pure TableLeafPage
        _ -> fail "Unknown page type"

isLeafPage :: PageType -> Bool
isLeafPage IndexLeafPage = True
isLeafPage TableLeafPage = True
isLeafPage _ = False

pageHeaderParser :: BG.Get (PageHeader, Word16)
pageHeaderParser = do
    pageType <- pageTypeParser
    firstFreeblockOffset <- BG.getWord16be
    numberOfCells <- BG.getWord16be
    cellContentAreaStartOffset <- BG.getWord16be
    fragmentedFreeBytes <- BG.getWord8
    rightmostPointer <-
        if isLeafPage pageType
            then pure Nothing
            else Just <$> BG.getWord32be
    pure
        ( PageHeader
            { _pageType = pageType
            , _firstFreeblockOffset = firstFreeblockOffset
            , _numberOfCells = numberOfCells
            , _cellContentAreaStartOffset = cellContentAreaStartOffset
            , _fragmentedFreeBytes = fragmentedFreeBytes
            , _rightmostPointer = rightmostPointer
            }
        , if isLeafPage pageType then 8 else 12
        )

type PageCellPayload = BL.ByteString

type RowId = Int64

data PageCell
    = TableLeafCell
        { _rowId :: RowId
        , _recordValues :: TableRecord
        }
    | TableInteriorCell
        { _leftChildPageNumber :: Word32
        , _rowId :: RowId
        }
    | IndexLeafCell
        { _rowId :: RowId
        , _recordValues :: TableRecord
        }
    | IndexInteriorCell
        { _leftChildPageNumber :: Word32
        , _rowId :: RowId
        , _recordValues :: TableRecord
        }
    deriving (Show)

type CellPointer = Word16

{- |
>>> :set -XBinaryLiterals
>>> BG.runGet variantParserWithSize $ BL.pack [0b10000001, 0b00101100]
(172,2)

>>> BG.runGet variantParserWithSize $ BL.pack [0x81, 0x80, 0x00]
(16384,3)
>>> BG.runGet variantParserWithSize $ BL.pack [0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00]
(144115188075856000,9)
-}
variantParserWithSize :: BG.Get (Word64, Word8)
variantParserWithSize = do
    go 0 0
  where
    hasMoreBytes :: Word8 -> Bool
    hasMoreBytes w = w .&. 0x80 /= 0

    go :: Word8 -> Word64 -> BG.Get (Word64, Word8)
    go i acc = do
        byte <- BG.getWord8
        if i < 8
            then
                let acc' = (acc `shiftL` 7) .|. fromIntegral (byte .&. 0x7F)
                 in if hasMoreBytes byte
                        then go (i + 1) acc'
                        else pure (acc', i + 1)
            else
                -- last byte, the 9th byte
                pure ((acc `shiftL` 8) .|. fromIntegral byte, 9)

variantParser :: BG.Get Word64
variantParser = fst <$> variantParserWithSize

signedVariantParser :: BG.Get Int64
signedVariantParser = fromIntegral . fst <$> variantParserWithSize

pageCellParser :: PageType -> BG.Get PageCell
pageCellParser pageType = do
    case pageType of
        TableLeafPage -> do
            -- looks like we don't need payload size because we not handling overflow
            payloadSize <- variantParser
            rowId <- signedVariantParser
            TableLeafCell rowId <$> recordParser
        TableInteriorPage -> do
            leftChildPageNumber <- BG.getWord32be
            TableInteriorCell leftChildPageNumber <$> signedVariantParser
        IndexLeafPage -> do
            payloadSize <- variantParser
            (values, rowId) <- indexPayloadParser
            pure $ IndexLeafCell rowId values
        IndexInteriorPage -> do
            leftChildPageNumber <- BG.getWord32be
            payloadSize <- variantParser
            (values, rowId) <- indexPayloadParser
            pure $ IndexInteriorCell leftChildPageNumber rowId values

pageCellsParser :: Word16 -> PageHeader -> BG.Get [PageCell]
pageCellsParser offset (PageHeader{_pageType = pageType, _numberOfCells = n}) = do
    cellPointers <- BG.lookAhead $ mapM (const BG.getWord16be) [0 .. n - 1]
    mapM
        ( \pointer ->
            BG.lookAhead $ do
                BG.skip $ fromIntegral $ pointer - offset
                pageCellParser pageType
        )
        cellPointers

type TableRecord = [TableRecordValue]

data TableRecordValue
    = NullRecord
    | NumericRecord Word64
    | Float64Record Double
    | InternalRecord
    | StringRecord Text
    | BlobRecord BL.ByteString
    deriving (Eq)

instance Ord TableRecordValue where
    compare :: TableRecordValue -> TableRecordValue -> Ordering
    compare NullRecord _ = LT
    compare _ NullRecord = GT
    compare (NumericRecord a) (NumericRecord b) = compare a b
    compare (Float64Record a) (Float64Record b) = compare a b
    compare (StringRecord a) (StringRecord b) = compare a b
    compare (BlobRecord a) (BlobRecord b) = compare a b

instance Show TableRecordValue where
    show :: TableRecordValue -> String
    show NullRecord = "NULL"
    show (StringRecord s) = T.unpack s
    show (Float64Record f) = show f
    show InternalRecord = ""
    show (BlobRecord _) = "<blob>"
    show (NumericRecord n) = show n

recordParser :: BG.Get TableRecord
recordParser = do
    (headerSize, consumedSize) <- variantParserWithSize
    columns <- recordHeaderParser (headerSize - fromIntegral consumedSize)
    mapM valueParser columns
  where
    valueParser :: Word64 -> BG.Get TableRecordValue
    valueParser typ = do
        case typ of
            0 -> pure NullRecord
            1 -> NumericRecord . fromIntegral <$> BG.getWord8
            2 -> NumericRecord . fromIntegral <$> BG.getWord16be
            3 -> NumericRecord . fromIntegral <$> getWord24
            4 -> NumericRecord . fromIntegral <$> BG.getWord32be
            5 -> NumericRecord . fromIntegral <$> getWord48
            6 -> NumericRecord . fromIntegral <$> BG.getWord64be
            7 -> Float64Record <$> BG.getDoublebe
            8 -> pure $ NumericRecord 0
            9 -> pure $ NumericRecord 1
            10 -> pure InternalRecord
            11 -> pure InternalRecord
            _ ->
                if even typ
                    then
                        BlobRecord <$> BG.getLazyByteString (fromIntegral $ (typ - 12) `div` 2)
                    else do
                        s <- BG.getLazyByteString (fromIntegral $ (typ - 13) `div` 2)
                        -- assuming encoding is latin1
                        pure $ StringRecord $ decodeLatin1 s

    getWord48 :: BG.Get Word64
    getWord48 = do
        a <- BG.getWord16be
        b <- BG.getWord32be
        pure $ (fromIntegral a `shiftL` 32) .|. fromIntegral b

    getWord24 :: BG.Get Word32
    getWord24 = do
        a <- BG.getWord8
        b <- BG.getWord16be
        pure $ (fromIntegral a `shiftL` 16) .|. fromIntegral b

    recordHeaderParser :: Word64 -> BG.Get [Word64]
    recordHeaderParser size =
        if size == 0
            then pure []
            else do
                (column, consumedSize) <- variantParserWithSize
                (column :) <$> recordHeaderParser (size - fromIntegral consumedSize)

indexPayloadParser :: BG.Get (TableRecord, RowId)
indexPayloadParser = do
    [indexValue, NumericRecord rowId] <- recordParser
    pure ([indexValue], fromIntegral rowId)

data ObjectType = TableObject | IndexObject | ViewObject | TriggerObject
    deriving (Show, Eq)

textToObjectType :: Text -> ObjectType
textToObjectType "table" = TableObject
textToObjectType "index" = IndexObject
textToObjectType "view" = ViewObject
textToObjectType "trigger" = TriggerObject
textToObjectType x = error $ "Unknown object type: " <> show x

data ObjectScema = ObjectScema
    { _objectType :: ObjectType
    , _objectName :: Text
    , _objectTblName :: Text
    , _objectRootPage :: Word64
    , _objectSql :: Text
    }
    deriving (Show)

pageParser :: Word16 -> BG.Get (PageHeader, [PageCell])
pageParser offset = do
    (header, headerSize) <- pageHeaderParser
    -- assume page header is always 8 bytes
    cells <- pageCellsParser (offset + headerSize) header
    pure (header, cells)

schemaTableParser :: BG.Get [ObjectScema]
schemaTableParser = do
    (_, cells) <- pageParser 100
    pure $
        mapMaybe
            ( \case
                TableLeafCell
                    { _recordValues =
                        [ StringRecord typ
                            , StringRecord name
                            , StringRecord tblName
                            , NumericRecord rootPage
                            , StringRecord sql
                            ]
                    } ->
                        Just
                            ObjectScema
                                { _objectType = textToObjectType typ
                                , _objectName = name
                                , _objectTblName = tblName
                                , _objectRootPage = rootPage
                                , _objectSql = sql
                                }
                _ -> Nothing
            )
            cells

isTable :: ObjectScema -> Bool
isTable ObjectScema{_objectType = TableObject} = True
isTable _ = False

readDbHeader :: FilePath -> IO DatabaseHeader
readDbHeader dbFilePath = do
    content <- BL.readFile dbFilePath
    pure $ BG.runGet databaseHeaderParser content

findTable :: FilePath -> Word16 -> Text -> IO ObjectScema
findTable dbFilePath pageSize tableName =
    mmapDbPage dbFilePath pageSize 1 $ do
        tables <- filter isTable <$> schemaTableParser
        case find (\obj -> _objectTblName obj == tableName) tables of
            Just table -> pure table
            _ -> fail $ "table \"" <> T.unpack tableName <> "\" not found"

findIndexObject :: FilePath -> Word16 -> Text -> Text -> IO (Maybe ObjectScema)
findIndexObject dbFilePath pageSize tableName columnName =
    mmapDbPage dbFilePath pageSize 1 $ do
        tables <- filter (\obj -> _objectType obj == IndexObject) <$> schemaTableParser
        pure $ find (\obj -> _objectTblName obj == tableName && _objectName obj == "idx_" <> tableName <> "_" <> columnName) tables

scanTable :: FilePath -> Word16 -> ObjectScema -> IO [PageCell]
scanTable dbFilePath pageSize table = do
    go (_objectRootPage table)
  where
    go :: Word64 -> IO [PageCell]
    go pageNumber = do
        (pageHeader, cells) <- mmapDbPage dbFilePath pageSize pageNumber (pageParser 0)
        case _pageType pageHeader of
            TableInteriorPage ->
                concat
                    <$> mapM
                        ( \case
                            TableInteriorCell leftChildPageNumber _ ->
                                go $ fromIntegral leftChildPageNumber
                            _ -> pure []
                        )
                        cells
            TableLeafPage ->
                pure cells

main :: IO ()
main = do
    (dbFilePath : cmd : _) <- getArgs
    dbHeader <- readDbHeader dbFilePath
    let pageSize = _dbPageSize dbHeader
    case cmd of
        ".dbinfo" -> do
            print $ "database page size: " <> show pageSize
            count <- mmapDbPage dbFilePath pageSize 1 $ do
                length . filter isTable <$> schemaTableParser
            print $ "number of tables: " <> show count
            pure ()
        ".tables" -> do
            names <- mmapDbPage dbFilePath pageSize 1 $ do
                tables <- filter isTable <$> schemaTableParser
                pure $ fmap _objectTblName tables
            IO.putStr $ T.unwords names
        sql -> do
            let expr = Sql.parseQueryExpr sqlDialet "" Nothing $ T.toStrict $ T.pack sql
            case expr of
                Right Sql.Select{qeSelectList = [(Sql.App [Sql.Name _ "count"] _, _)], qeFrom = [Sql.TRSimple [Sql.Name _ tableName]]} -> do
                    table <- findTable dbFilePath pageSize (T.fromStrict tableName)
                    n <- selectCountStar dbFilePath pageSize table
                    print n
                Right
                    Sql.Select
                        { qeSelectList = idens
                        , qeFrom = [Sql.TRSimple [Sql.Name _ tableName]]
                        , qeWhere = whereCond
                        } -> do
                        let columnNames = (\(Sql.Iden [Sql.Name _ columnName], _) -> T.fromStrict columnName) <$> idens
                        table <- findTable dbFilePath pageSize (T.fromStrict tableName)
                        case whereCond of
                            Just (Sql.BinOp (Sql.Iden [Sql.Name _ "id"]) [Sql.Name _ "="] (Sql.NumLit n)) ->
                                selectByRowId dbFilePath pageSize table columnNames (read $ T.unpack $ T.fromStrict n)
                            Just (Sql.BinOp (Sql.Iden [Sql.Name _ name]) [Sql.Name _ "="] equalTo) ->
                                let equalTo' =
                                        case equalTo of
                                            (Sql.StringLit _ _ s) ->
                                                StringRecord $ T.fromStrict s
                                            (Sql.NumLit n) ->
                                                NumericRecord $ read $ T.unpack $ T.fromStrict n
                                 in selectColumns
                                        dbFilePath
                                        pageSize
                                        table
                                        (T.fromStrict tableName)
                                        columnNames
                                        (Just (T.fromStrict name, equalTo'))
                            Just _ -> error "not support"
                            Nothing -> selectColumns dbFilePath pageSize table (T.fromStrict tableName) columnNames Nothing
                Right x -> print x
                x -> do
                    error "execute sql error"

sqlDialet :: Dialect
sqlDialet =
    ansi2011{diAutoincrement = True}

selectByRowId :: FilePath -> Word16 -> ObjectScema -> [Text] -> RowId -> IO ()
selectByRowId dbFilePath pageSize table columnNames rowId = do
    values <- go rowId (_objectRootPage table)
    case values of
        Just values -> do
            let schemaColumnNames = getSchemaColumnNames (_objectSql table)
            let columnIndexes = mapMaybe (`elemIndex` schemaColumnNames) columnNames
            printColumnValues columnIndexes rowId values
        _ -> putStrLn "not found"
  where
    go :: RowId -> Word64 -> IO (Maybe TableRecord)
    go rowId pageNumber = do
        (pageHeader, cells) <- mmapDbPage dbFilePath pageSize pageNumber (pageParser 0)
        -- FIXME: use binary search
        case dropWhile (\c -> _rowId c < rowId) cells of
            (TableInteriorCell leftChildPageNumber _) : _ ->
                go rowId $ fromIntegral leftChildPageNumber
            (TableLeafCell rid values) : _ ->
                if rid == rowId
                    then pure $ Just values
                    else pure Nothing
            [] -> go rowId $ fromIntegral $ fromJust $ _rightmostPointer pageHeader
            _ -> pure Nothing

selectColumns :: FilePath -> Word16 -> ObjectScema -> Text -> [Text] -> Maybe (Text, TableRecordValue) -> IO ()
selectColumns dbFilePath pageSize table tableName columnNames whereCond = do
    case whereCond of
        Just (name, equalTo) -> do
            foundIndexObject <- findIndexObject dbFilePath pageSize tableName name
            case foundIndexObject of
                Just indexObject -> do
                    rowIds <- selectByIndex equalTo $ _objectRootPage indexObject
                    forM_ rowIds $ selectByRowId dbFilePath pageSize table columnNames
                _ ->
                    selectByScanTable (\values -> lookup name values == Just equalTo)
        _ -> selectByScanTable $ const True
  where
    selectByScanTable :: ([(Text, TableRecordValue)] -> Bool) -> IO ()
    selectByScanTable pred = do
        cells <- scanTable dbFilePath pageSize table
        let schemaColumnNames = getSchemaColumnNames (_objectSql table)
        let columnIndexes = mapMaybe (`elemIndex` schemaColumnNames) columnNames
        mapM_
            ( \case
                TableLeafCell rowId payload ->
                    ( when (pred (zip schemaColumnNames payload)) $
                        printColumnValues columnIndexes rowId payload
                    )
            )
            cells

    headEquals :: (Eq a) => a -> [a] -> Bool
    headEquals x (h : _) = x == h
    headEquals _ _ = False

    headLessThan :: (Ord a) => a -> [a] -> Bool
    headLessThan x (h : _) = h < x
    headLessThan _ _ = False

    takeWhileAnd1More pred xs =
        case span pred xs of
            (ys, []) -> ys
            (ys, h : _) -> h : ys

    selectByIndex :: TableRecordValue -> Word64 -> IO [RowId]
    selectByIndex indexValue pageNumber = do
        (pageHeader, cells) <- mmapDbPage dbFilePath pageSize pageNumber (pageParser 0)
        case _pageType pageHeader of
            IndexLeafPage ->
                cells
                    & dropWhile (\c -> headLessThan indexValue (_recordValues c))
                    & takeWhile (\c -> headEquals indexValue (_recordValues c))
                    & map _rowId
                    & pure
            IndexInteriorPage -> do
                -- print $ "record values: " ++ show (map _recordValues cells)
                let cs =
                        cells
                            & dropWhile (\c -> headLessThan indexValue (_recordValues c))
                            & takeWhileAnd1More (\c -> headEquals indexValue (_recordValues c))
                case cs of
                    [] ->
                        case _rightmostPointer pageHeader of
                            Nothing -> pure []
                            Just rightmostPointer ->
                                selectByIndex indexValue $ fromIntegral rightmostPointer
                    _ -> do
                        ress <- forM cs $ \(IndexInteriorCell leftChildPageNumber rowId values) -> do
                            res <- selectByIndex indexValue $ fromIntegral leftChildPageNumber
                            if headEquals indexValue values
                                then pure $ rowId : res
                                else pure res

                        pure $ concat ress
            _ -> pure []

printColumnValues :: [Int] -> RowId -> [TableRecordValue] -> IO ()
printColumnValues columnIndexes rowId payload =
    putStrLn $
        intercalate "|" $
            fmap
                ( \i ->
                    if i == 0
                        then show rowId
                        else
                            show $ payload !! i
                )
                columnIndexes

getSchemaColumnNames :: Text -> [Text]
getSchemaColumnNames sql =
    case Sql.parseStatement sqlDialet "" Nothing $ T.toStrict sql of
        Right (Sql.CreateTable _ columns) ->
            mapMaybe
                ( \case
                    Sql.TableColumnDef (Sql.ColumnDef (Sql.Name _ name) _ _ _) ->
                        Just $ T.fromStrict name
                    _ -> Nothing
                )
                columns
        Left err ->
            []

selectCountStar :: FilePath -> Word16 -> ObjectScema -> IO Int
selectCountStar dbFilePath pageSize table = do
    (_, cells) <- mmapDbPage dbFilePath pageSize (_objectRootPage table) (pageParser 0)
    pure $ length cells

mmapDbPage :: (Show a) => FilePath -> Word16 -> Word64 -> BG.Get a -> IO a
mmapDbPage path pageSize pageIndex parser = do
    bs <-
        mmapFileByteString
            path
            ( Just $
                if pageIndex == 1
                    then (100, fromIntegral pageSize - 100)
                    else
                        ( fromIntegral pageSize * fromIntegral (pageIndex - 1)
                        , fromIntegral pageSize
                        )
            )
    pure $ BG.runGet parser $ BL.fromStrict bs
