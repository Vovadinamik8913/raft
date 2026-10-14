package dto

type RequestVoteRequest struct {
	Term        int    `json:"term"`
	CandidateID string `json:"candidate_id"`
}

type RequestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}
