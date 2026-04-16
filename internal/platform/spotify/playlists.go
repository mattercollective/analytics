package spotify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/mattercollective/analytics-engine/internal/model"
)

const webAPIBase = "https://api.spotify.com/v1"

// PlaylistFetcher fetches playlist metadata and tracks from the Spotify Web API.
type PlaylistFetcher struct {
	httpClient *http.Client
}

// NewPlaylistFetcher creates a playlist fetcher using the same OAuth client.
func NewPlaylistFetcher(httpClient *http.Client) *PlaylistFetcher {
	return &PlaylistFetcher{httpClient: httpClient}
}

// GetPlaylistFetcher returns a PlaylistFetcher using the client's auth.
func (c *Client) GetPlaylistFetcher() *PlaylistFetcher {
	return NewPlaylistFetcher(c.httpClient)
}

// FetchPlaylistMetadata retrieves playlist info (name, description, followers, image).
func (pf *PlaylistFetcher) FetchPlaylistMetadata(ctx context.Context, playlistID string) (*model.PlaylistUpsert, error) {
	url := fmt.Sprintf("%s/playlists/%s?fields=id,name,description,owner(display_name),followers(total),images,external_urls,tracks(total)", webAPIBase, playlistID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := pf.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("spotify API error %d: %s", resp.StatusCode, string(body))
	}

	var pl spotifyPlaylist
	if err := json.NewDecoder(resp.Body).Decode(&pl); err != nil {
		return nil, fmt.Errorf("decode playlist: %w", err)
	}

	curatorType := "user"
	if pl.Owner.DisplayName == "Spotify" {
		curatorType = "editorial"
	}

	var imageURL *string
	if len(pl.Images) > 0 {
		imageURL = &pl.Images[0].URL
	}

	platformURL := pl.ExternalURLs.Spotify
	followers := int64(pl.Followers.Total)
	trackCount := pl.Tracks.Total

	return &model.PlaylistUpsert{
		PlatformID:    "spotify",
		ExternalID:    pl.ID,
		Name:          pl.Name,
		Description:   &pl.Description,
		CuratorName:   &pl.Owner.DisplayName,
		CuratorType:   &curatorType,
		FollowerCount: &followers,
		TrackCount:    &trackCount,
		ImageURL:      imageURL,
		PlatformURL:   &platformURL,
	}, nil
}

// FetchPlaylistTracks retrieves all tracks in a playlist with their positions.
func (pf *PlaylistFetcher) FetchPlaylistTracks(ctx context.Context, playlistID string) ([]playlistTrack, error) {
	var allTracks []playlistTrack
	offset := 0
	limit := 100

	for {
		url := fmt.Sprintf("%s/playlists/%s/tracks?offset=%d&limit=%d&fields=items(track(id,name,external_ids,artists(name))),total,next",
			webAPIBase, playlistID, offset, limit)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		resp, err := pf.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("execute request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("spotify API error %d: %s", resp.StatusCode, string(body))
		}

		var page spotifyTracksPage
		json.NewDecoder(resp.Body).Decode(&page)
		resp.Body.Close()

		for i, item := range page.Items {
			if item.Track.ID == "" {
				continue
			}
			position := offset + i + 1 // 1-indexed
			isrc := item.Track.ExternalIDs.ISRC
			allTracks = append(allTracks, playlistTrack{
				SpotifyID: item.Track.ID,
				ISRC:      isrc,
				Name:      item.Track.Name,
				Position:  position,
			})
		}

		if page.Next == nil || len(page.Items) == 0 {
			break
		}
		offset += limit
	}

	return allTracks, nil
}

// SnapshotPlaylist fetches a playlist and returns position data ready for storage.
func (pf *PlaylistFetcher) SnapshotPlaylist(ctx context.Context, playlistID string, today time.Time) (*model.PlaylistUpsert, []playlistTrack, error) {
	meta, err := pf.FetchPlaylistMetadata(ctx, playlistID)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch metadata: %w", err)
	}

	tracks, err := pf.FetchPlaylistTracks(ctx, playlistID)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch tracks: %w", err)
	}

	return meta, tracks, nil
}

// -- Spotify Web API response types --

type spotifyPlaylist struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	Description  string `json:"description"`
	Owner        struct {
		DisplayName string `json:"display_name"`
	} `json:"owner"`
	Followers struct {
		Total int `json:"total"`
	} `json:"followers"`
	Images []struct {
		URL string `json:"url"`
	} `json:"images"`
	ExternalURLs struct {
		Spotify string `json:"spotify"`
	} `json:"external_urls"`
	Tracks struct {
		Total int `json:"total"`
	} `json:"tracks"`
}

type spotifyTracksPage struct {
	Items []struct {
		Track struct {
			ID          string `json:"id"`
			Name        string `json:"name"`
			ExternalIDs struct {
				ISRC string `json:"isrc"`
			} `json:"external_ids"`
			Artists []struct {
				Name string `json:"name"`
			} `json:"artists"`
		} `json:"track"`
	} `json:"items"`
	Total int     `json:"total"`
	Next  *string `json:"next"`
}

type playlistTrack struct {
	SpotifyID string
	ISRC      string
	Name      string
	Position  int
}
