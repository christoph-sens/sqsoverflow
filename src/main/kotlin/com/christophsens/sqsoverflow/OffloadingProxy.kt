package com.christophsens.sqsoverflow

import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy

/**
 * Creates a proxy implementing interface [T] as it exists at runtime. Calls to [offloadedOperations] go
 * to [offloading], all other interface calls to [delegate]. Unlike Kotlin interface delegation, which
 * generates forwarding methods at compile time, the proxy also forwards operations that a newer version
 * of [T] added after this library was compiled, instead of failing with `AbstractMethodError`.
 *
 * Suspend functions work unchanged: their `Continuation` is just another argument.
 */
internal inline fun <reified T : Any> offloadingProxy(offloading: T, delegate: T, offloadedOperations: Set<String>): T =
    Proxy.newProxyInstance(
        T::class.java.classLoader,
        arrayOf(T::class.java),
        OffloadingInvocationHandler(offloading, delegate, offloadedOperations),
    ) as T

internal class OffloadingInvocationHandler(
    private val offloading: Any,
    private val delegate: Any,
    private val offloadedOperations: Set<String>,
) : InvocationHandler {
    override fun invoke(proxy: Any, method: Method, args: Array<out Any?>?): Any? {
        if (method.declaringClass == Any::class.java) return objectMethod(proxy, method, args)

        val target = if (method.name in offloadedOperations) offloading else delegate
        return try {
            method.invoke(target, *(args ?: emptyArray()))
        } catch (e: InvocationTargetException) {
            throw e.targetException
        }
    }

    private fun objectMethod(proxy: Any, method: Method, args: Array<out Any?>?): Any =
        when (method.name) {
            "equals" -> proxy === args?.firstOrNull()
            "hashCode" -> System.identityHashCode(proxy)
            else -> "SqsExtendedClient(delegate=$delegate)"
        }
}
