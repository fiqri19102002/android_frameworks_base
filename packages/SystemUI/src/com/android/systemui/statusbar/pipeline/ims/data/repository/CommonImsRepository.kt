/*
 * Copyright (C) 2022 The Android Open Source Project
 * Copyright (C) 2024 The LibreMobileOS Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.android.systemui.statusbar.pipeline.ims.data.repository

import android.content.Context
import android.database.ContentObserver
import android.provider.Settings
import android.telephony.SubscriptionInfo
import android.telephony.SubscriptionManager
import com.android.systemui.common.coroutine.ConflatedCallbackFlow.conflatedCallbackFlow
import com.android.systemui.dagger.SysUISingleton
import com.android.systemui.dagger.qualifiers.Application
import com.android.systemui.dagger.qualifiers.Background
import com.android.systemui.statusbar.pipeline.ims.data.model.ImsIconModel
import com.android.systemui.statusbar.pipeline.ims.data.model.ImsStateModel
import javax.inject.Inject
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.asExecutor
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapLatest
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.withContext

interface CommonImsRepository {
    val imsStates: StateFlow<List<ImsStateModel>>
    val imsIconState: StateFlow<ImsIconModel>
    fun getRepoForSubId(subId: Int): ImsRepository
}

@OptIn(ExperimentalCoroutinesApi::class)
@SysUISingleton
class CommonImsRepositoryImpl
@Inject
constructor(
    private val subscriptionManager: SubscriptionManager,
    @Background private val bgDispatcher: CoroutineDispatcher,
    @Application private val scope: CoroutineScope,
    private val imsRepoFactory: ImsRepositoryImpl.Factory,
    private val context: Context,
): CommonImsRepository {

    private var subIdRepositoryCache: MutableMap<Int, ImsRepository> =
        mutableMapOf()

    private val mobileSubscriptionsChangeEvent: Flow<Unit> = conflatedCallbackFlow {
        val callback =
            object : SubscriptionManager.OnSubscriptionsChangedListener() {
                override fun onSubscriptionsChanged() {
                    trySend(Unit)
                }
            }

        subscriptionManager.addOnSubscriptionsChangedListener(
            bgDispatcher.asExecutor(),
            callback,
        )

        awaitClose { subscriptionManager.removeOnSubscriptionsChangedListener(callback) }
    }

    private val subscriptions: StateFlow<List<Int>> =
        mobileSubscriptionsChangeEvent
            .mapLatest { fetchSubscriptionsList().map { it.subscriptionId } }
            .onEach { ids -> dropUnusedReposFromCache(ids) }
            .distinctUntilChanged()
            .stateIn(scope, started = SharingStarted.WhileSubscribed(), listOf())

    private fun dropUnusedReposFromCache(newIds: List<Int>) {
        // Remove any connection repository from the cache that isn't in the new set of IDs. They
        // will get garbage collected once their subscribers go away
        subIdRepositoryCache =
            subIdRepositoryCache.filter { checkSub(it.key, newIds) }.toMutableMap()
    }

    /**
     * True if the checked subId is in the list of current subs
     *
     * @param checkedSubIds the list to validate [subId] against. To invalidate the cache, pass in the
     *   new subscription list. Otherwise use [subscriptions.value] to validate a subId against the
     *   current known subscriptions
     */
    private fun checkSub(subId: Int, checkedSubIds: List<Int>): Boolean {
        checkedSubIds.forEach {
            if (it == subId) {
                return true
            }
        }
        return false
    }

    private suspend fun fetchSubscriptionsList(): List<SubscriptionInfo> =
            withContext(bgDispatcher) { subscriptionManager.completeActiveSubscriptionInfoList }

    override val imsStates: StateFlow<List<ImsStateModel>> =
        subscriptions
            .map { subIds ->
                subIds.map { getRepoForSubId(it) }
            }
            .flatMapLatest { repos ->
                combine(repos.map { it.imsState }) { values ->
                    values.toList()
                }
            }
            .stateIn(scope, started = SharingStarted.WhileSubscribed(), listOf())

    override val imsIconState: StateFlow<ImsIconModel> = conflatedCallbackFlow {
        var showHdIcon = false
        var showVowifiIcon = false
        
        // Read initial values from icon hide list
        val hideList = getIconHideList()
        showHdIcon = !hideList.contains(KEY_HD_ICON)
        showVowifiIcon = !hideList.contains(KEY_VOWIFI_ICON)
        
        // Send initial state
        trySend(
            ImsIconModel(
                showHdIcon = showHdIcon,
                showVowifiIcon = showVowifiIcon
            )
        )

        // Monitor for settings changes
        val settingsObserver = object : ContentObserver(null) {
            override fun onChange(selfChange: Boolean) {
                super.onChange(selfChange)
                val hideList = getIconHideList()
                showHdIcon = !hideList.contains(KEY_HD_ICON)
                showVowifiIcon = !hideList.contains(KEY_VOWIFI_ICON)
                trySend(
                    ImsIconModel(
                        showHdIcon = showHdIcon,
                        showVowifiIcon = showVowifiIcon
                    )
                )
            }
        }

        // Register observer for icon hide list changes
        context.contentResolver.registerContentObserver(
            Settings.Secure.getUriFor(ICON_HIDE_LIST),
            false,
            settingsObserver
        )

        awaitClose { 
            context.contentResolver.unregisterContentObserver(settingsObserver)
        }
    }.stateIn(
        scope,
        started = SharingStarted.WhileSubscribed(),
        initialValue = ImsIconModel()
    )

    private fun getIconHideList(): Set<String> {
        val hideListStr = Settings.Secure.getString(context.contentResolver, ICON_HIDE_LIST)
        return if (hideListStr.isNullOrEmpty()) {
            emptySet()
        } else {
            hideListStr.split(",").toSet()
        }
    }

    override fun getRepoForSubId(subId: Int): ImsRepository =
        getOrCreateRepoForSubId(subId)

    private fun getOrCreateRepoForSubId(subId: Int) =
        subIdRepositoryCache[subId]
            ?: createRepositoryForSubId(subId).also { subIdRepositoryCache[subId] = it }

    private fun createRepositoryForSubId(subId: Int): ImsRepository {
        return imsRepoFactory.build(subId)
    }

    private companion object {
        const val ICON_HIDE_LIST = "icon_blacklist"
        const val KEY_HD_ICON = "hd_calling"
        const val KEY_VOWIFI_ICON = "vowifi"
    }
}
