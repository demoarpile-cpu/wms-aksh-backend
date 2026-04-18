const { Readable } = require('stream');
const supplierProductService = require('../services/supplierProductService');
const csv = require('csv-parser');
const fs = require('fs');

/** Query string or multipart field: forceCreate / createNew */
function parseForceCreate(req) {
  const raw = String(
    req.query?.forceCreate ?? req.query?.createNew ?? req.body?.forceCreate ?? req.body?.createNew ?? ''
  )
    .toLowerCase()
    .trim();
  return raw === '1' || raw === 'true' || raw === 'yes';
}

function readFileAsUtf8String(filePath) {
  const buf = fs.readFileSync(filePath);
  // UTF-16 LE BOM (common on Windows)
  if (buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xfe) {
    return buf.slice(2).toString('utf16le');
  }
  // UTF-16 BE BOM
  if (buf.length >= 2 && buf[0] === 0xfe && buf[1] === 0xff) {
    const body = buf.slice(2);
    const copy = Buffer.from(body);
    copy.swap16();
    return copy.toString('utf16le');
  }
  let s = buf.toString('utf8');
  if (s.length > 0 && s.charCodeAt(0) === 0xfeff) s = s.slice(1);
  return s;
}

function detectSeparatorFromFirstLine(firstLine) {
  if (!firstLine) return ',';
  const tabs = (firstLine.match(/\t/g) || []).length;
  const commas = (firstLine.match(/,/g) || []).length;
  const semis = (firstLine.match(/;/g) || []).length;
  const pipes = (firstLine.match(/\|/g) || []).length;
  if (tabs > 0 && tabs >= commas && tabs >= semis && tabs >= pipes) return '\t';
  if (pipes > 0 && pipes >= commas && pipes >= semis) return '|';
  if (semis > commas) return ';';
  return ',';
}

/** Excel (EU) sometimes saves "sep=;" as the first line — strip before parsing/counting lines. */
function stripLeadingExcelSepDirective(text) {
  const lines = text.split(/\r?\n/);
  if (!lines.length) return text;
  const first = lines[0].replace(/^\uFEFF/, '').trim();
  const m = first.match(/^sep\s*=\s*(.)$/i);
  if (!m) return text;
  const ch = m[1];
  if (ch === ',' || ch === ';' || ch === '\t' || ch === '|') {
    return lines.slice(1).join('\n');
  }
  return text;
}

/** Excel / Word sometimes emit “curly” quotes; they break RFC CSV quoting and merge rows. */
function normalizeCsvTypography(text) {
  return String(text || '')
    .replace(/\uFEFF/g, '')
    .replace(/[\u201C\u201D\u201E\u00AB\u00BB\u2033\u2036]/g, '"')
    .replace(/[\u2018\u2019\u201A\u2032\u2035]/g, "'");
}

/**
 * Split one CSV line into cells (handles "..." quotes). Used when csv-parser merges many physical
 * lines into fewer records due to stray / unbalanced double quotes.
 */
function splitCsvLineCells(line, sep) {
  const out = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQuotes) {
      if (c === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += c;
      }
    } else if (c === '"') {
      inQuotes = true;
    } else if (c === sep) {
      out.push(cur);
      cur = '';
    } else {
      cur += c;
    }
  }
  out.push(cur);
  return out;
}

/** Row objects same shape as csv-parser (header keys from first non-empty line). */
function parseCsvManualDelimited(text, sep) {
  const lines = text.split(/\r?\n/).map((l) => l.replace(/\r$/, ''));
  const nonEmpty = lines.filter((l) => l.trim().length > 0);
  if (nonEmpty.length < 2) return [];
  const rawHeaders = splitCsvLineCells(nonEmpty[0], sep);
  const headers = rawHeaders.map((h) =>
    String(h || '')
      .replace(/^\uFEFF/, '')
      .replace(/^["'\s]+|["'\s]+$/g, '')
      .trim()
  );
  const rows = [];
  for (let i = 1; i < nonEmpty.length; i++) {
    const cells = splitCsvLineCells(nonEmpty[i], sep);
    const row = {};
    for (let j = 0; j < headers.length; j++) {
      const key = headers[j];
      if (!key) continue;
      row[key] = cells[j] !== undefined ? cells[j] : '';
    }
    rows.push(row);
  }
  return rows;
}

/** Same folding idea as supplierProductService (headers only). */
function foldHeaderKey(k) {
  return String(k || '')
    .replace(/^\uFEFF/, '')
    .replace(/\r/g, '')
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, '');
}

/** Recognizable supplier / product column names after foldHeaderKey (keep in sync with supplierProductService). */
const USABLE_SUP_HINTS = [
  'supplierid',
  'suppliername',
  'suppliercode',
  'supplier',
  'vendorid',
  'vendorname',
  'vendorcode',
  'vendor',
  'partnerid',
  'partnername',
  'partnercode',
  'partner',
];
const USABLE_PROD_HINTS = [
  'productid',
  'sku',
  'internalsku',
  'internalcode',
  'productsku',
  'productcode',
  'itemcode',
  'stockcode',
  'stylecode',
  'itemsku',
  'mastersku',
  'articlenumber',
  'itemnumber',
  'materialnumber',
  'material',
];

function headerKeySetUsable(hs) {
  if (hs.size < 1) return false;
  /** Excel slice with only vendor fields: supplierSku + pack/cost — updates existing mapping by supplierSku. */
  const hasVendorSku = hs.has('suppliersku') || hs.has('vendorsku');
  if (hasVendorSku) return true;
  if (hs.size < 2) return false;
  const hasSup = USABLE_SUP_HINTS.some((x) => hs.has(x));
  const hasProd = USABLE_PROD_HINTS.some((x) => hs.has(x));
  return hasSup && hasProd;
}

/** True when parsed columns look like our template (supplier + product columns present). */
function parseLooksUsable(rows) {
  if (!rows.length || !rows[0] || typeof rows[0] !== 'object') return false;
  const hs = new Set(Object.keys(rows[0]).map(foldHeaderKey));
  return headerKeySetUsable(hs);
}

/** Pick delimiter where the header row splits into the most “known” columns (fixes EU ; vs ,). */
function orderSeparatorsByHeaderLine(firstLine) {
  const candidates = [',', ';', '\t', '|'];
  const scored = candidates.map((sep) => {
    const cells = splitCsvLineCells(firstLine, sep);
    const trimmed = cells.map((c) => String(c || '').trim()).filter(Boolean);
    const usable = headerKeySetUsable(new Set(cells.map((c) => foldHeaderKey(c)).filter(Boolean)));
    const score = (usable ? 1_000_000 : 0) + trimmed.length * 10_000 + cells.length;
    return { sep, score };
  });
  scored.sort((a, b) => b.score - a.score);
  return scored.map((s) => s.sep);
}

function parseWithSeparator(text, sep) {
  return new Promise((resolve, reject) => {
    const rows = [];
    Readable.from([text])
      .pipe(
        csv({
          separator: sep,
          strict: false,
          mapHeaders: ({ header }) =>
            String(header || '')
              .replace(/^\uFEFF/, '')
              .replace(/^["'\s]+|["'\s]+$/g, '')
              .trim(),
        })
      )
      .on('data', (row) => rows.push(row))
      .on('end', () => resolve(rows))
      .on('error', reject);
  });
}

function importMetricForRows(rows) {
  const mappable = supplierProductService.countMappableImportRows(rows);
  const headerOk = parseLooksUsable(rows);
  return { mappable, headerOk, len: rows.length };
}

/** Prefer more rows that map to supplier+SKU after normalization (fixes wrong delimiter → half rows “empty”). */
function isBetterImportMetric(candidate, incumbent) {
  if (candidate.mappable !== incumbent.mappable) return candidate.mappable > incumbent.mappable;
  if (candidate.headerOk !== incumbent.headerOk) return candidate.headerOk && !incumbent.headerOk;
  return candidate.len > incumbent.len;
}

/**
 * Try comma / semicolon / tab / pipe for both streaming and line-based parses, then pick the
 * variant where the most data rows contain a recognizable supplier + product (after column mapping).
 */
async function parseSupplierCsvFromPath(filePath) {
  const text = normalizeCsvTypography(stripLeadingExcelSepDirective(readFileAsUtf8String(filePath)));
  const nonEmptyLines = text.split(/\r?\n/).filter((l) => l.trim().length > 0);
  const firstLine = nonEmptyLines[0] || '';
  const primarySep = detectSeparatorFromFirstLine(firstLine);
  const headerOrderedSeps = orderSeparatorsByHeaderLine(firstLine);
  /** Physical non-empty lines minus header — compare to parsed row count to spot broken CSV / multiline cells. */
  const approxNonEmptyDataLines = Math.max(0, nonEmptyLines.length - 1);
  const trySeparators = [...new Set([...headerOrderedSeps, primarySep, ',', ';', '\t', '|'])];

  const candidates = [];
  for (const sep of trySeparators) {
    const streamRows = await parseWithSeparator(text, sep);
    candidates.push({ rows: streamRows, sep, source: 'csv-parser' });
    const manualRows = parseCsvManualDelimited(text, sep);
    if (manualRows.length) {
      candidates.push({ rows: manualRows, sep, source: 'line-split-fallback' });
    }
  }

  let best = { rows: [], sep: primarySep, source: 'csv-parser' };
  let bestMetric = { mappable: -1, headerOk: false, len: -1 };

  for (const cand of candidates) {
    if (!cand.rows || !cand.rows.length) continue;
    const m = importMetricForRows(cand.rows);
    if (isBetterImportMetric(m, bestMetric)) {
      best = cand;
      bestMetric = m;
    }
  }

  const valid = parseLooksUsable(best.rows);

  return {
    rows: best.rows,
    parseMeta: {
      separator: best.sep,
      approxNonEmptyDataLines,
      parseHeaderUsable: valid,
      parserSource: best.source,
      lineSplitFallbackUsed: best.source === 'line-split-fallback',
      mappableRowEstimate: Math.max(0, bestMetric.mappable),
      winningParsedRowCount: best.rows.length,
    },
  };
}

async function list(req, res, next) {
  try {
    const data = await supplierProductService.list(req.user, req.query);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function listMappedProductsBySupplier(req, res, next) {
  try {
    const data = await supplierProductService.listMappedProductsBySupplier(req.user, req.params.supplierId);
    res.json({ success: true, data });
  } catch (err) {
    next(err);
  }
}

async function bulkUpload(req, res, next) {
  if (req.file) {
    const lowerName = String(req.file.originalname || '').toLowerCase();
    if (lowerName.endsWith('.xlsx') || lowerName.endsWith('.xls')) {
      try {
        if (fs.existsSync(req.file.path)) fs.unlinkSync(req.file.path);
      } catch (_) {
        /* ignore */
      }
      return res.status(400).json({
        success: false,
        message: 'Please upload a .csv file (in Excel: File → Save As → CSV UTF-8).',
      });
    }

    const filePath = req.file.path;
    let rows;
    let parseMeta = { separator: ',', approxNonEmptyDataLines: 0 };
    try {
      const parsed = await parseSupplierCsvFromPath(filePath);
      rows = parsed.rows;
      parseMeta = parsed.parseMeta || parseMeta;
    } catch (err) {
      try {
        if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
      } catch (_) {
        /* ignore */
      }
      return res.status(400).json({ success: false, message: err.message || 'Failed to read CSV file' });
    }

    try {
      if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    } catch (_) {
      /* ignore */
    }

    if (!rows.length) {
      const msg =
        'No data rows were parsed. Save as CSV UTF-8 with a header row and at least one data row. Use commas or tabs between columns.';
      return res.json({
        success: true,
        results: {
          created: 0,
          updated: 0,
          errors: [msg],
          totalRowsReceived: 0,
          emptySkipped: 0,
          succeededRows: 0,
          skipSummary: supplierProductService.summarizeBulkSkips([msg], 0),
          importDiagnostics: {
            fileRowsParsed: 0,
            approxNonEmptyDataLines: parseMeta.approxNonEmptyDataLines,
            csvSeparator: parseMeta.separator,
            headerKeys: [],
            endpoint: 'multipart-csv-v2',
            hint: 'CSV parser returned zero rows — file may be empty, wrong encoding, or not real CSV.',
          },
        },
      });
    }

    try {
      const chunkSize = req.query.chunkSize != null ? Number(req.query.chunkSize) : undefined;
      const forceCreate = parseForceCreate(req);
      const results = await supplierProductService.bulkUpload(rows, req.user, {
        chunkSize: Number.isFinite(chunkSize) && chunkSize > 0 ? chunkSize : undefined,
        forceCreate,
      });
      const headerKeys = rows[0]
        ? Object.keys(rows[0]).map((k) => String(k).replace(/\r/g, '').trim())
        : [];
      results.importDiagnostics = {
        fileRowsParsed: rows.length,
        approxNonEmptyDataLines: parseMeta.approxNonEmptyDataLines,
        csvSeparator: parseMeta.separator,
        parseHeaderUsable: parseMeta.parseHeaderUsable,
        parserSource: parseMeta.parserSource,
        lineSplitFallbackUsed: Boolean(parseMeta.lineSplitFallbackUsed),
        mappableRowEstimate: parseMeta.mappableRowEstimate,
        winningParsedRowCount: parseMeta.winningParsedRowCount,
        headerKeys,
        endpoint: 'multipart-csv-v2',
      };
      if (parseMeta.parseHeaderUsable === false && rows.length > 0) {
        results.importDiagnostics.hint =
          'Could not confirm standard columns (supplier + product) from the header row. If rows are missing, re-save as CSV UTF-8 or try comma-separated export instead of semicolon (or vice versa).';
      }
      const approx = parseMeta.approxNonEmptyDataLines;
      const mapped = parseMeta.mappableRowEstimate;
      if (approx != null && mapped != null && approx > mapped + 1 && rows.length > 0) {
        results.importDiagnostics.hint =
          (results.importDiagnostics.hint ? `${results.importDiagnostics.hint} ` : '') +
          `File has about ${approx} data line(s) but only ${mapped} row(s) had supplier+product values after column mapping — check delimiter (comma vs ;) and column names, or use Export CSV from this page.`;
      }
      return res.json({ success: true, results });
    } catch (err) {
      return res.status(400).json({ success: false, message: err.message });
    }
  }

  try {
    const body = req.body;
    const mappings = Array.isArray(body)
      ? body
      : body && typeof body === 'object' && Array.isArray(body.data)
        ? body.data
        : [];
    const chunkSize = req.query.chunkSize != null ? Number(req.query.chunkSize) : undefined;
    const forceCreate = parseForceCreate(req);
    const results = await supplierProductService.bulkUpload(mappings, req.user, {
      chunkSize: Number.isFinite(chunkSize) && chunkSize > 0 ? chunkSize : undefined,
      forceCreate,
    });
    if (mappings.length && mappings[0] && typeof mappings[0] === 'object') {
      results.importDiagnostics = {
        fileRowsParsed: mappings.length,
        headerKeys: Object.keys(mappings[0]).map((k) => String(k).replace(/\r/g, '').trim()),
        endpoint: 'json-body',
      };
    }
    res.json({ success: true, results });
  } catch (err) {
    next(err);
  }
}

async function remove(req, res, next) {
  try {
    await supplierProductService.remove(req.params.id, req.user);
    res.json({ success: true, message: 'Mapping deleted' });
  } catch (err) {
    if (err.message === 'Mapping not found') return res.status(404).json({ success: false, message: err.message });
    next(err);
  }
}

module.exports = { list, listMappedProductsBySupplier, bulkUpload, remove };
