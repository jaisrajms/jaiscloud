/**
 * Cursor pagination for the GCP console APIs.
 *
 * GCP list endpoints return one page at a time with an opaque, forward-only
 * `nextPageToken`; the console's `GcpDataTable` paginates (and sorts/filters)
 * client-side, so the API layer drains the cursor and hands the pages the full
 * set. Each GCP list client keeps an optional `{ pageToken }`: passing one
 * returns a single page (the raw cursor semantics, for a future server-driven
 * table), omitting it drains every page.
 */

/** The paging envelope every GCP list response carries. */
export interface PagedResponse {
  nextPageToken?: string
}

export interface FetchAllOptions {
  /**
   * Safety cap on the number of pages fetched. Prevents an unbounded loop if a
   * backend keeps emitting cursors; the provider default page size is 1000.
   */
  maxPages?: number
}

/**
 * Follow `nextPageToken` and return every page response. `fetchPage` is called
 * with the cursor from the previous page (undefined for the first). Iteration
 * stops when a page has no token, or when a page echoes back the token it was
 * given (a backend that advertises a cursor it does not honor), so a buggy
 * server cannot spin the loop. `maxPages` bounds a server that keeps minting
 * fresh tokens.
 */
export async function fetchAllPageResponses<TPage extends PagedResponse>(
  fetchPage: (pageToken?: string) => Promise<TPage>,
  options: FetchAllOptions = {},
): Promise<TPage[]> {
  const maxPages = options.maxPages ?? 100
  const pages: TPage[] = []
  let token: string | undefined
  for (let page = 0; page < maxPages; page++) {
    const response = await fetchPage(token)
    pages.push(response)
    const next = response.nextPageToken
    if (!next || next === token) break
    token = next
  }
  return pages
}

/**
 * Follow `nextPageToken` and flatten every page into one item array via
 * `select`. See {@link fetchAllPageResponses} for the cursor rules.
 */
export async function fetchAllPages<TPage extends PagedResponse, TItem>(
  fetchPage: (pageToken?: string) => Promise<TPage>,
  select: (page: TPage) => TItem[],
  options: FetchAllOptions = {},
): Promise<TItem[]> {
  const pages = await fetchAllPageResponses(fetchPage, options)
  return pages.flatMap(select)
}
