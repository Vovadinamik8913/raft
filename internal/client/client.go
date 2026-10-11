package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Vovadinamik8913/raft/internal/dto"
)

type Client struct {
	httpClient *http.Client
}

func New() *Client {
	return &Client{httpClient: &http.Client{Timeout: 400 * time.Millisecond}}
}

func (c *Client) RequestVote(ctx context.Context, peer string, req dto.RequestVoteRequest) (dto.RequestVoteResponse, error) {
	var resp dto.RequestVoteResponse
	err := c.postRPC(ctx, peer+"/request_vote", req, &resp)
	return resp, err
}

func (c *Client) AppendEntries(ctx context.Context, peer string, req dto.AppendEntriesRequest) (dto.AppendEntriesResponse, error) {
	var resp dto.AppendEntriesResponse
	err := c.postRPC(ctx, peer+"/append_entries", req, &resp)
	return resp, err
}

func (c *Client) postRPC(ctx context.Context, endpoint string, req, response any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("RPC %s returned HTTP %d", endpoint, resp.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 1<<20))
	if err := decoder.Decode(response); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("invalid RPC response: trailing JSON")
	}
	return nil
}
