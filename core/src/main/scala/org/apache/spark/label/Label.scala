package org.apache.spark.label

/**
 * @author liuchang
 */
class Label(_name:String) extends Serializable {
  val name = _name
  
  def /\ (label:Label):Label = {
    if (this.isBot) label
    else if(this.isTop || label.isBot) this
    else if (name == label.name) this
    else
      Label.top
  }
  
  def \/ (label:Label):Label = {
    if (this.isBot) this
    else if(this.isTop || label.isBot) label
    else if (name == label.name) this
    else
      Label.bot
  }
  
  def != (label:Label) = name != label.name
  
  def == (label:Label) = name == label.name
  
  def isBot:Boolean = name == Label.BOT_NAME
  
  def isTop:Boolean = name == Label.TOP_NAME
}

object Label {
  def BOT_NAME = "\\bot"
  
  def TOP_NAME = "\\top"
  
  def bot: Label = new Label(BOT_NAME)
  
  def top: Label = new Label(TOP_NAME)
}