package repl

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/takeuchi-shogo/go-example-database/internal/session"
)

const (
	PROMPT          = "godb> "
	PROMPT_CONTINUE = ">> "
	VERSION         = "0.1.0"
)

type Repl struct {
	input   io.Reader
	output  io.Writer
	session session.Session
}

func NewRepl(input io.Reader, output io.Writer, session session.Session) *Repl {
	return &Repl{input: input, output: output, session: session}
}

func (r *Repl) Run() {
	scanner := bufio.NewScanner(r.input) // 入力をスキャンする

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		r.printGoodBye()
		r.exit()
	}()

	r.printWelcome()

	for {
		r.printPrompt()

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		r.executeCommand(line)
	}
}

func (r *Repl) printWelcome() {
	fmt.Fprintf(r.output, "Welcome to godb v%s\n", VERSION)
	fmt.Fprintln(r.output, "Type \".help\" for usage hints, \".exit\" to quit.")
	fmt.Fprintln(r.output)
}

func (r *Repl) printPrompt() {
	fmt.Fprint(r.output, PROMPT)
}

func (r *Repl) executeCommand(line string) {
	if strings.HasPrefix(line, ".") {
		if r.handleCommand(line) {
			return
		}
	}
	// コマンドがない場合はSQLを評価
	r.eval(line)
}

func (r *Repl) handleCommand(line string) bool {
	switch line {
	case ".exit", ".quit":
		r.exit()
		return true
	case ".help":
		r.printHelp()
		return true
	}
	return false
}

func (r *Repl) exit() {
	os.Exit(0)
}

func (r *Repl) printHelp() {
	r.print("Commands:")
	r.print("  .help, .h     Show this help")
	r.print("  .exit, .quit  Exit the REPL")
	r.print("  Ctrl+C        Exit the REPL")
	r.print("  Ctrl+D        Exit the REPL (EOF)")
	r.print("SQL Statements:")
	r.print("  SELECT   - Query data")
	r.print("  INSERT   - Insert data")
	r.print("  UPDATE   - Update data")
	r.print("  DELETE   - Delete data")
	r.print("  CREATE   - Create table")
	r.print("  EXPLAIN  - Show query plan")
}

func (r *Repl) print(s string, args ...interface{}) {
	fmt.Fprintf(r.output, s+"\n", args...)
}

func (r *Repl) eval(input string) {
	result, err := r.session.Execute(input)
	if err != nil {
		fmt.Fprintln(r.output, "Error:", err)
		return
	}
	fmt.Fprintln(r.output, result.String())
}

var goodbyeMessages = []string{
	"See you later! 👋",
	"Goodbye! Thanks for using godb.",
	"Bye! Happy coding!",
}

func (r *Repl) printGoodBye() {
	// NOTE:
	// Go 1.20+ では math/rand の rand.Seed は非推奨。
	// ここではローカルな RNG を作って、終了メッセージの選択だけに利用する。
	randomGenerator := rand.New(rand.NewSource(time.Now().UnixNano()))
	fmt.Fprintln(r.output, goodbyeMessages[randomGenerator.Intn(len(goodbyeMessages))])
}
