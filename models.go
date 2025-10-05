package main

import (
	"time"

	"github.com/whyrusleeping/market/models"
	"gorm.io/gorm"
)

type Repo = models.Repo
type Post = models.Post
type Follow = models.Follow
type Block = models.Block
type Repost = models.Repost
type List = models.List
type ListItem = models.ListItem
type ListBlock = models.ListBlock
type Profile = models.Profile
type ThreadGate = models.ThreadGate
type FeedGenerator = models.FeedGenerator
type Image = models.Image
type PostGate = models.PostGate
type StarterPack = models.StarterPack

type Like struct {
	ID      uint `gorm:"primarykey"`
	Created time.Time
	Indexed time.Time
	Author  uint   `gorm:"uniqueIndex:idx_likes_rkeyauthor"`
	Rkey    string `gorm:"uniqueIndex:idx_likes_rkeyauthor"`
	Subject uint
	Cid     string
}

type Notification struct {
	gorm.Model
	For uint

	Author uint
	Source string
	Kind   string
}
