# 动作

动作是用来向外部系统写入数据的。动作可以用来写控制数据以触发一个动作，还可以用来写状态数据并保存在外部存储器中。

一个规则可以有多个动作，不同的动作可以是同一个动作类型。

## 结果编码

动作的结是一个字符串。默认情况下，它将被编码为 json 字符串。用户可以通过设置`dataTemplate` 来改变格式，它利用 go 模板语法将结果格式化为字符串。为了更详细地控制结果的格式，用户可以开发一个动作扩展。

## 参考阅读

- [动作参考](../guide/sinks/overview.md)