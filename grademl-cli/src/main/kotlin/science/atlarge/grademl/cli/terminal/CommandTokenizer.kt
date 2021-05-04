package science.atlarge.grademl.cli.terminal

object CommandTokenizer {

    fun tokenizeCommand(commandArguments: List<String>): List<CommandToken> {
        // Iterate over "words" to parse options and extract arguments
        val tokens = mutableListOf<CommandToken>()
        var nextWordIndex = 0
        while (nextWordIndex < commandArguments.size) {
            // Interpret the next word
            val wordIndex = nextWordIndex++
            val word = commandArguments[wordIndex]
            when {
                // Check for invalid option format, starting with more than two dashes
                word.startsWith("---") -> {
                    tokens.add(
                        InvalidToken(
                            word,
                            "Invalid format for command option: \"$word\"",
                            wordIndex,
                            0,
                            word.length
                        )
                    )
                }
                // Check for a marker closing the option list
                word == "--" -> {
                    tokens.add(EndOfOptionsToken(wordIndex, 0, word.length))
                    while (nextWordIndex < commandArguments.size) {
                        val nextWord = commandArguments[nextWordIndex]
                        tokens.add(ArgumentToken(nextWord, nextWordIndex, 0, nextWord.length))
                        nextWordIndex++
                    }
                }
                // Check for a long option
                word.startsWith("--") -> {
                    if ('=' in word) {
                        val keyValueMatch = word.split('=', limit = 2)
                        tokens.add(
                            LongOptionToken(
                                keyValueMatch[0].trimStart('-'),
                                wordIndex,
                                0,
                                keyValueMatch[0].length
                            )
                        )
                        tokens.add(
                            ArgumentToken(
                                keyValueMatch[1],
                                wordIndex,
                                keyValueMatch[0].length + 1,
                                word.length
                            )
                        )
                    } else {
                        tokens.add(LongOptionToken(word.trimStart('-'), wordIndex, 0, word.length))
                    }
                }
                // Check for a short option
                word.startsWith("-") && word != "-" -> {
                    if (word.length > 2) {
                        tokens.add(
                            InvalidToken(
                                word,
                                "Contraction of short options is not supported: \"$word\"",
                                wordIndex,
                                0,
                                word.length
                            )
                        )
                    } else {
                        tokens.add(ShortOptionToken(word[1], wordIndex, 0, word.length))
                    }
                }
                // Treat everything else as an argument token
                else -> tokens.add(ArgumentToken(word, wordIndex, 0, word.length))
            }
        }
        return tokens
    }

}

sealed class CommandToken {
    abstract val wordIndex: Int
    abstract val wordStartOffset: Int
    abstract val wordEndOffset: Int
}

data class ShortOptionToken(
    val option: Char,
    override val wordIndex: Int,
    override val wordStartOffset: Int,
    override val wordEndOffset: Int
) : CommandToken()

data class LongOptionToken(
    val option: String,
    override val wordIndex: Int,
    override val wordStartOffset: Int,
    override val wordEndOffset: Int
) : CommandToken()

data class ArgumentToken(
    val value: String,
    override val wordIndex: Int,
    override val wordStartOffset: Int,
    override val wordEndOffset: Int
) : CommandToken()

data class EndOfOptionsToken(
    override val wordIndex: Int,
    override val wordStartOffset: Int,
    override val wordEndOffset: Int
) : CommandToken()

data class InvalidToken(
    val word: String,
    val errorMessage: String,
    override val wordIndex: Int,
    override val wordStartOffset: Int,
    override val wordEndOffset: Int
) : CommandToken()