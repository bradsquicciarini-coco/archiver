package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/foxglove/mcap/go/mcap"
)

type stringList []string

func (s *stringList) String() string {
	return fmt.Sprintf("%v", []string(*s))
}

func (s *stringList) Set(value string) error {
	if value == "" {
		return errors.New("channel cannot be empty")
	}
	*s = append(*s, value)
	return nil
}

type channelKey struct {
	topic    string
	encoding string
}

type markableSchema struct {
	*mcap.Schema
	written bool
}

type markableChannel struct {
	*mcap.Channel
	written bool
}

func main() {
	var inputPath string
	var outputPath string
	var channels stringList

	flag.StringVar(&inputPath, "in", "", "input MCAP file (default: stdin)")
	flag.StringVar(&outputPath, "out", "", "output MCAP file (default: stdout)")
	flag.Var(&channels, "keep-channel", "channel to include in the form <topic>:<message_encoding> (repeatable)")
	flag.Parse()

	reader, closeReader, err := openInput(inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open input: %v\n", err)
		os.Exit(1)
	}
	if closeReader != nil {
		defer closeReader()
	}

	writer, closeWriter, err := openOutput(outputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open output: %v\n", err)
		os.Exit(1)
	}
	if closeWriter != nil {
		defer func() {
			if closeErr := closeWriter(); closeErr != nil {
				fmt.Fprintf(os.Stderr, "failed to close output: %v\n", closeErr)
			}
		}()
	}

	if err := filterByChannels(reader, writer, channels); err != nil {
		fmt.Fprintf(os.Stderr, "filter failed: %v\n", err)
		os.Exit(1)
	}
}

func openInput(path string) (io.Reader, func() error, error) {
	if path == "" {
		stat, err := os.Stdin.Stat()
		if err != nil {
			return nil, nil, err
		}
		if stat.Mode()&os.ModeCharDevice != 0 {
			return nil, nil, errors.New("no input file provided and stdin is a tty")
		}
		return os.Stdin, nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return f, f.Close, nil
}

func openOutput(path string) (io.Writer, func() error, error) {
	if path == "" {
		return os.Stdout, nil, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return f, f.Close, nil
}

func parseKeepChannels(values []string) (map[string]map[string]struct{}, error) {
	channelSet := make(map[string]map[string]struct{}, len(values))
	for _, ch := range values {
		sep := -1
		for i := 0; i < len(ch); i++ {
			if ch[i] == ':' {
				sep = i
				break
			}
		}
		if sep <= 0 || sep == len(ch)-1 {
			return nil, fmt.Errorf("invalid keep-channel %q, expected <topic>:<message_encoding>", ch)
		}
		topic := ch[:sep]
		encoding := ch[sep+1:]
		if _, ok := channelSet[topic]; !ok {
			channelSet[topic] = make(map[string]struct{})
		}
		channelSet[topic][encoding] = struct{}{}
	}
	return channelSet, nil
}

func filterByChannels(r io.Reader, w io.Writer, channels []string) error {
	channelSet, err := parseKeepChannels(channels)
	if err != nil {
		return err
	}

	writer, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Compression: mcap.CompressionNone,
		Chunked:     true,
		ChunkSize:   4 * 1024 * 1024,
	})
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "failed to close mcap writer: %v\n", closeErr)
		}
	}()

	lexer, err := mcap.NewLexer(r, &mcap.LexerOptions{
		ValidateChunkCRCs: true,
	})
	if err != nil {
		return err
	}

	buf := make([]byte, 1024)
	schemas := make(map[uint16]markableSchema)
	channelsByID := make(map[uint16]markableChannel)

	for {
		token, data, err := lexer.Next(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if len(data) > len(buf) {
			buf = data
		}

		switch token {
		case mcap.TokenHeader:
			header, err := mcap.ParseHeader(data)
			if err != nil {
				return err
			}
			if err := writer.WriteHeader(header); err != nil {
				return err
			}
		case mcap.TokenSchema:
			schema, err := mcap.ParseSchema(data)
			if err != nil {
				return err
			}
			schemas[schema.ID] = markableSchema{Schema: schema}
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				return err
			}
			if len(channelSet) == 0 {
				channelsByID[channel.ID] = markableChannel{Channel: channel}
				break
			}
			encodingSet, hasRule := channelSet[channel.Topic]
			if !hasRule {
				channelsByID[channel.ID] = markableChannel{Channel: channel}
				break
			}
			if _, ok := encodingSet[channel.MessageEncoding]; ok {
				channelsByID[channel.ID] = markableChannel{Channel: channel}
			}
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(data)
			if err != nil {
				return err
			}
			channel, ok := channelsByID[message.ChannelID]
			if !ok {
				continue
			}
			if !channel.written {
				if channel.SchemaID != 0 {
					schema, ok := schemas[channel.SchemaID]
					if !ok {
						return fmt.Errorf(
							"encountered channel %q with unknown schema ID %d",
							channel.Topic,
							channel.SchemaID,
						)
					}
					if !schema.written {
						if err := writer.WriteSchema(schema.Schema); err != nil {
							return err
						}
						schemas[channel.SchemaID] = markableSchema{Schema: schema.Schema, written: true}
					}
				}
				if err := writer.WriteChannel(channel.Channel); err != nil {
					return err
				}
				channelsByID[message.ChannelID] = markableChannel{Channel: channel.Channel, written: true}
			}
			if err := writer.WriteMessage(message); err != nil {
				return err
			}
		case mcap.TokenDataEnd, mcap.TokenFooter:
			return nil
		case mcap.TokenChunk:
			return errors.New("expected lexer to remove chunk records from input stream")
		case mcap.TokenMetadata:
			continue
		case mcap.TokenError:
			return errors.New("received error token but lexer did not return error on Next")
		}
	}
}
