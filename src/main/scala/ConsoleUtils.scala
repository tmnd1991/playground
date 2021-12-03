object ConsoleUtils {

  val ANSI_RESET = "\u001B[0m"
  val ANSI_BLACK = "\u001B[30m"
  val ANSI_RED = "\u001B[31m"
  val ANSI_GREEN = "\u001B[32m"
  val ANSI_YELLOW = "\u001B[33m"
  val ANSI_BLUE = "\u001B[34m"
  val ANSI_PURPLE = "\u001B[35m"
  val ANSI_CYAN = "\u001B[36m"
  val ANSI_WHITE = "\u001B[37m"

  def red(s: String): String = ANSI_RED + s + ANSI_RESET
  def green(s: String): String = ANSI_GREEN + s + ANSI_RESET
  def yellow(s: String): String = ANSI_YELLOW + s + ANSI_RESET
  def blue(s: String): String = ANSI_BLUE + s + ANSI_RESET
  def purple(s: String): String = ANSI_PURPLE + s + ANSI_RESET
  def cyan(s: String): String = ANSI_CYAN + s + ANSI_RESET
  def white(s: String): String = ANSI_WHITE + s + ANSI_RESET
}
