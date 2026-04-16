package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// SyncISRCClientMappings pulls ISRC → organization mappings from CMS
// asset_reports_with_org view and creates analytics.client_assets entries
// linking existing ISRC-based assets to their owning clients.
func (c *CMSSync) SyncISRCClientMappings(ctx context.Context) (int, error) {
	c.logger.Info().Msg("syncing ISRC → client mappings from CMS")

	type isrcOrg struct {
		DisplayISRC  string `json:"display_isrc"`
		YourISRC     string `json:"your_isrc"`
		OtherISRC    string `json:"other_isrc"`
		Organization string `json:"organization"`
	}

	// Paginate through the CMS view
	var rows []isrcOrg
	offset := 0
	limit := 1000

	for {
		u := fmt.Sprintf("%s/rest/v1/asset_reports_with_org?select=display_isrc,your_isrc,other_isrc,organization&display_isrc=not.is.null&organization=not.is.null&limit=%d&offset=%d",
			c.apiURL, limit, offset)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return 0, err
		}
		req.Header.Set("apikey", c.apiKey)
		req.Header.Set("Authorization", "Bearer "+c.apiKey)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			return 0, fmt.Errorf("CMS API error %d: %s", resp.StatusCode, string(body))
		}

		var page []isrcOrg
		if err := json.Unmarshal(body, &page); err != nil {
			return 0, err
		}

		rows = append(rows, page...)
		if len(page) < limit {
			break
		}
		offset += limit

		if offset%10000 == 0 {
			c.logger.Info().Int("fetched", len(rows)).Msg("CMS pagination progress")
		}
	}

	c.logger.Info().Int("rows", len(rows)).Msg("fetched all ISRC/org rows from CMS")

	// Build ISRC → org set (deduped)
	isrcToOrg := make(map[string]string, len(rows))
	for _, row := range rows {
		if row.Organization == "" {
			continue
		}
		candidates := []string{row.DisplayISRC, row.YourISRC}
		for _, o := range strings.Fields(row.OtherISRC) {
			candidates = append(candidates, o)
		}

		for _, isrc := range candidates {
			isrc = strings.TrimSpace(isrc)
			if len(isrc) < 10 {
				continue
			}
			if _, exists := isrcToOrg[isrc]; !exists {
				isrcToOrg[isrc] = row.Organization
			}
		}
	}

	c.logger.Info().Int("unique_isrcs", len(isrcToOrg)).Msg("deduped ISRC mappings")

	// For each ISRC, look up asset_id and create client_assets mapping
	created := 0
	missing := 0
	processed := 0

	for isrc, orgID := range isrcToOrg {
		processed++

		var assetID string
		err := c.pool.QueryRow(ctx,
			`SELECT asset_id FROM public.asset_identifiers
			 WHERE identifier_type = 'isrc' AND identifier_value = $1 AND effective_to IS NULL
			 LIMIT 1`,
			isrc,
		).Scan(&assetID)
		if err != nil {
			missing++
			continue
		}

		tag, err := c.pool.Exec(ctx,
			`INSERT INTO analytics.client_assets (client_id, asset_id)
			 VALUES ($1, $2)
			 ON CONFLICT DO NOTHING`,
			orgID, assetID,
		)
		if err != nil {
			continue
		}
		if tag.RowsAffected() > 0 {
			created++
		}

		if processed%10000 == 0 {
			c.logger.Info().Int("processed", processed).Int("created", created).Int("missing", missing).Msg("ISRC mapping progress")
		}
	}

	c.logger.Info().Int("mappings_created", created).Int("isrcs_missing_assets", missing).Msg("ISRC client mapping complete")
	return created, nil
}
